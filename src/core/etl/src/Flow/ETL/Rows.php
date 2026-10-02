<?php

declare(strict_types=1);

namespace Flow\ETL;

use Countable;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Php\ValueColumn;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\JoinSide;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Rows\RowEquality;
use Flow\ETL\Rows\RowHashes;
use Flow\ETL\Schema\Formatter\InlineSchemaFormatter;
use Flow\ETL\Schema\SimilarNames;
use Flow\ETL\Sort\RowOrder;
use Flow\Floe\FrameDecoder;
use Flow\Floe\FrameEncoder;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Value\Json;
use Generator;

use function array_diff_key;
use function array_fill;
use function array_key_exists;
use function array_keys;
use function array_shift;
use function array_slice;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function implode;
use function max;
use function min;
use function sprintf;

/**
 * @type RowsPayload = array{schema: Schema, frame: string}
 */
final class Rows implements Countable
{
    /**
     * @param array<array-key, Column> $columns keyed and ordered by $schema
     */
    private function __construct(
        private Schema $schema,
        private array $columns,
        private int $count,
    ) {}

    /**
     * @return RowsPayload
     */
    public function __serialize(): array
    {
        return ['schema' => $this->schema, 'frame' => $this->encodeFrame()];
    }

    /**
     * @param RowsPayload $data
     */
    public function __unserialize(array $data): void
    {
        $rows = (new FrameDecoder())->decode($data['frame'], $data['schema'], new AdaptiveBackend());

        $this->schema = $rows->schema;
        $this->columns = $rows->columns;
        $this->count = $rows->count;
    }

    public static function empty(Schema $schema): self
    {
        $backend = new AdaptiveBackend();
        $columns = [];

        foreach ($schema->definitions() as $name => $definition) {
            $columns[$name] = $backend->builder($definition)->finish();
        }

        return new self($schema, $columns, 0);
    }

    /**
     * @param array<array-key, Column> $columns keyed and ordered by $schema, every one of $count rows
     *
     * @throws InvalidArgumentException
     * @throws SchemaMismatchException a NOT NULL column holds a null
     */
    public static function fromColumns(Schema $schema, array $columns, int $count): self
    {
        if (array_keys($columns) !== array_keys($schema->definitions())) {
            throw new InvalidArgumentException(sprintf(
                'Rows::fromColumns() expects columns [%s], got [%s]',
                implode(', ', array_keys($schema->definitions())),
                implode(', ', array_keys($columns)),
            ));
        }

        foreach ($columns as $name => $column) {
            if ($column->count() !== $count) {
                throw new InvalidArgumentException(sprintf(
                    'Column "%s" holds %d rows, the batch %d',
                    $name,
                    $column->count(),
                    $count,
                ));
            }
        }

        foreach ($schema->definitions() as $name => $definition) {
            if ($columns[$name] instanceof ValueColumn) {
                throw ColumnMismatchException::untypedColumn($definition);
            }

            if (!$definition->isNullable() && $columns[$name]->nullCount() > 0) {
                $index = 0;

                while (!$columns[$name]->isNull($index)) {
                    $index++;
                }

                throw new SchemaMismatchException($index, ColumnMismatchException::valueDoesNotMatch(
                    $definition,
                    null,
                ));
            }
        }

        return new self($schema, $columns, $count);
    }

    /**
     * @param int<1, max> $size
     *
     * @return \Generator<int, Rows>
     */
    public function chunks(int $size): Generator
    {
        for ($offset = 0; $offset < $this->count; $offset += $size) {
            yield $this->slice($offset, min($size, $this->count - $offset));
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public function column(string $name): Column
    {
        if (array_key_exists($name, $this->columns)) {
            return $this->columns[$name];
        }

        $suggestions = (new SimilarNames())->closestTo($name, array_values($this->schema->references()->names()));

        throw new InvalidArgumentException(
            $suggestions === []
                ? "Column \"{$name}\" does not exist."
                : "Column \"{$name}\" does not exist. Did you mean one of the following? [\""
                . implode('", "', $suggestions)
                . '"]',
        );
    }

    /**
     * @return array<array-key, Column> schema order
     */
    public function columns(): array
    {
        return $this->columns;
    }

    public function encodeFrame(): string
    {
        return (new FrameEncoder())->encode($this);
    }

    /**
     * Every input carries this schema; zero-row inputs add no rows.
     *
     * @throws InvalidArgumentException
     */
    public function concat(self ...$others): self
    {
        $inputs = $this->count > 0 ? [$this] : [];
        $count = $this->count;

        foreach ($others as $other) {
            if (!$other->schema->isSame($this->schema)) {
                throw InvalidArgumentException::because(
                    'Cannot merge Rows with different schemas: [%s] and [%s]',
                    (new InlineSchemaFormatter())->format($this->schema),
                    (new InlineSchemaFormatter())->format($other->schema),
                );
            }

            if ($other->count > 0) {
                $inputs[] = $other;
                $count += $other->count;
            }
        }

        $head = array_shift($inputs);

        if ($head === null) {
            return self::empty($this->schema);
        }

        $backend = new AdaptiveBackend();
        $columns = [];

        foreach ($this->schema->definitions() as $name => $definition) {
            $first = $head->columns[$name];
            $rest = [];
            $sameClass = true;

            foreach ($inputs as $input) {
                $part = $input->columns[$name];
                $rest[] = $part;
                $sameClass = $sameClass && $part::class === $first::class;
            }

            if ($sameClass) {
                $columns[$name] = $first->concat(...$rest);

                continue;
            }

            $builder = $backend->builder($definition);

            foreach ([$first, ...$rest] as $part) {
                $builder->appendTake($part, range(0, $part->count() - 1));
            }

            $columns[$name] = $builder->finish();
        }

        return new self($this->schema, $columns, $count);
    }

    public function count(): int
    {
        return $this->count;
    }

    public function diffLeft(self $rows): self
    {
        return $this->gather((new RowEquality())->notIn($this, $rows, $this->schema));
    }

    public function diffRight(self $rows): self
    {
        // the surviving rows come from the right side, so the right side's schema describes them
        return $rows->gather((new RowEquality())->notIn($rows, $this, $this->schema));
    }

    public function drop(int $size): self
    {
        if ($size < 0) {
            throw new InvalidArgumentException('Size must be greater than or equal to 0');
        }

        if ($size === 0) {
            return $this;
        }

        return $this->slice(min($size, $this->count), $this->count - min($size, $this->count));
    }

    public function dropRight(int $size): self
    {
        if ($size < 0) {
            throw new InvalidArgumentException('Size must be greater than or equal to 0');
        }

        if ($size === 0) {
            return $this;
        }

        return $this->slice(0, $this->count - min($size, $this->count));
    }

    /**
     * @param list<int> $indices rows to keep, in the order given
     *
     * @throws InvalidArgumentException
     */
    public function gather(array $indices): self
    {
        if ($indices !== [] && (min($indices) < 0 || max($indices) >= $this->count)) {
            foreach ($indices as $index) {
                if ($index < 0 || $index >= $this->count) {
                    throw new InvalidArgumentException(sprintf(
                        'Rows::gather() index %d is outside a batch of %d rows',
                        $index,
                        $this->count,
                    ));
                }
            }
        }

        $columns = [];

        foreach ($this->columns as $name => $column) {
            $columns[$name] = $column->take($indices);
        }

        return new self($this->schema, $columns, count($indices));
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()): string
    {
        return $algorithm->hash(implode('', (new RowHashes())->of($this, $algorithm)));
    }

    /**
     * @param int $count - Count of rows to return. Must be >= 0.
     *
     * @throws InvalidArgumentException When count is negative
     * @throws InvalidTypeException When count is not an integer     */
    public function head(int $count): self
    {
        $count = type_integer()->assert($count);

        if ($count < 0) {
            throw new InvalidArgumentException('Count must be greater than or equal to 0');
        }

        return $this->slice(0, min($count, $this->count));
    }

    public function isEmpty(): bool
    {
        return $this->count === 0;
    }

    public function joinCross(self $right, string $joinPrefix = 'joined_'): self
    {
        $schema = (new JoinSchema($joinPrefix))->cross($this->schema, $right->schema);
        $left = [];
        $rightIndices = [];

        for ($i = 0; $i < $this->count; $i++) {
            for ($j = 0; $j < $right->count; $j++) {
                $left[] = $i;
                $rightIndices[] = $j;
            }
        }

        return (new RowMerger($joinPrefix))
            ->merge($this->gather($left), $right->gather($rightIndices))
            ->project($schema);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinInner(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::inner);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeft(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::left);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeftAnti(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::left_anti);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinRight(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::right);
    }

    /**
     * Adopts $schema, checking every column whose definition it changes (Definition::matches per value).
     *
     * @throws SchemaMismatchException
     */
    public function matchTo(Schema $schema): self
    {
        if ($schema->isSame($this->schema)) {
            return new self($schema, $this->columns, $this->count);
        }

        foreach ($this->schema->definitions() as $definition) {
            $name = $definition->entry()->name();

            if ($this->count > 0 && $schema->findDefinition($name) === null) {
                throw new SchemaMismatchException(0, ColumnMismatchException::unexpectedColumn($name));
            }
        }

        $backend = new AdaptiveBackend();
        $columns = [];
        $changed = [];

        foreach ($schema->definitions() as $name => $definition) {
            $own = $this->schema->findDefinition($definition->entry()->name());

            if ($own === null) {
                if (!$definition->isNullable()) {
                    if ($this->count > 0) {
                        throw new SchemaMismatchException(0, ColumnMismatchException::missingColumn($definition));
                    }

                    $columns[$name] = $backend->builder($definition)->finish();

                    continue;
                }

                $columns[$name] = $backend->constant($definition, null, $this->count);

                continue;
            }

            if ($own->isSame($definition)) {
                $columns[$name] = $this->columns[$name];

                continue;
            }

            $changed[$name] = $definition;
        }

        for ($i = 0; $i < $this->count; $i++) {
            foreach ($changed as $name => $definition) {
                // @mago-ignore analysis:mixed-assignment
                $value = $this->columns[$name]->value($i);

                if ($value === null ? !$definition->isNullable() : !$definition->matches($value)) {
                    throw new SchemaMismatchException($i, ColumnMismatchException::valueDoesNotMatch(
                        $definition,
                        $value,
                    ));
                }
            }
        }

        foreach ($changed as $name => $definition) {
            try {
                $columns[$name] = $this->columns[$name]->withType($definition->type());
            } catch (InvalidArgumentException) {
                $builder = $backend->builder($definition);
                $builder->appendMany($this->columns[$name]->values());
                $columns[$name] = $builder->finish();
            }
        }

        $ordered = [];

        foreach ($schema->definitions() as $name => $_) {
            $ordered[$name] = $columns[$name];
        }

        return self::fromColumns($schema, $ordered, $this->count);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function merge(self $rows): self
    {
        if ($this->isEmpty()) {
            return $rows;
        }

        if ($rows->isEmpty()) {
            return $this;
        }

        if (!$this->schema->isSame($rows->schema())) {
            throw InvalidArgumentException::because(
                'Cannot merge Rows with different schemas: [%s] and [%s]',
                (new InlineSchemaFormatter())->format($this->schema),
                (new InlineSchemaFormatter())->format($rows->schema),
            );
        }

        return $this->concat($rows);
    }

    /**
     * Drops the columns $schema does not declare and adopts it. Widening or retyping the batch is
     * not a projection - that goes through matchTo().
     *
     * @throws SchemaMismatchException
     */
    public function project(Schema $schema): self
    {
        $definitions = [];
        $columns = [];

        foreach ($schema->definitions() as $name => $definition) {
            $own = $this->schema->findDefinition($definition->entry()->name());

            if ($own !== null) {
                $definitions[] = $own;
                $columns[$name] = $this->columns[$name];
            }
        }

        return (new self(new Schema(...$definitions), $columns, $this->count))->matchTo($schema);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return list<mixed>
     */
    public function reduceToArray(string|Reference $reference): array
    {
        if ($this->count === 0) {
            return [];
        }

        return $this->column($reference instanceof Reference ? $reference->base() : $reference)->values();
    }

    public function remove(int $offset): self
    {
        if ($offset < 0 || $offset >= $this->count) {
            throw new InvalidArgumentException("Rows does not have {$offset} offset");
        }

        return $this->gather(array_values(array_diff_key(range(0, $this->count - 1), [$offset => true])));
    }

    public function reverse(): self
    {
        $indices = [];

        for ($i = $this->count - 1; $i >= 0; $i--) {
            $indices[] = $i;
        }

        return $this->gather($indices);
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function slice(int $offset, int $length): self
    {
        if ($offset < 0 || $length < 0 || ($offset + $length) > $this->count) {
            throw new InvalidArgumentException(sprintf(
                'Rows::slice(%d, %d) is outside a batch of %d rows',
                $offset,
                $length,
                $this->count,
            ));
        }

        $columns = [];

        foreach ($this->columns as $name => $column) {
            $columns[$name] = $column->slice($offset, $length);
        }

        return new self($this->schema, $columns, $length);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortAscending(string|Reference $reference): self
    {
        return $this->sortBy(
            (new UnresolvedReference($reference instanceof Reference ? $reference->base() : $reference))->asc(),
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortBy(Reference ...$references): self
    {
        if ($this->count === 0) {
            return $this;
        }

        $order = new RowOrder(array_values($references));

        return $this->gather($order->permutation($order->keys($this), $this->count));
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortDescending(string|Reference $reference): self
    {
        return $this->sortBy(
            (new UnresolvedReference($reference instanceof Reference ? $reference->base() : $reference))->desc(),
        );
    }

    /**
     * @param int $count - Count of rows to return. Must be >= 0.
     *
     * @throws InvalidArgumentException When count is negative
     * @throws InvalidTypeException When count is not an integer     */
    public function tail(int $count): self
    {
        $count = type_integer()->assert($count);

        if ($count < 0) {
            throw new InvalidArgumentException('Count must be greater than or equal to 0');
        }

        if ($count >= $this->count) {
            return $this;
        }

        return $this->slice($this->count - $count, $count);
    }

    public function take(int $size): self
    {
        if ($size < 0) {
            throw new InvalidArgumentException('Size must be greater than or equal to 0');
        }

        return $this->slice(0, min($size, $this->count));
    }

    public function takeRight(int $size): self
    {
        if ($size < 0) {
            throw new InvalidArgumentException('Size must be greater than or equal to 0');
        }

        return $this->slice($this->count - min($size, $this->count), min($size, $this->count))->reverse();
    }

    /**
     * @return array<int, array<array-key, mixed>>
     */
    public function toArray(bool $withKeys = true): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        $array = $this->count === 0 ? [] : array_fill(0, $this->count, []);

        foreach ($this->columns as $name => $column) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $i => $value) {
                if ($value instanceof Json) {
                    $value = $value->toArray();
                }

                $withKeys ? ($array[$i][$name] = $value) : ($array[$i][] = $value);
            }
        }

        return $array;
    }

    public function unique(): self
    {
        return $this->gather((new RowEquality())->unique($this));
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return array<array-key, mixed> the logical row, schema order
     */
    public function values(int $i): array
    {
        if ($i < 0 || $i >= $this->count) {
            throw InvalidArgumentException::because('Row %d does not exist in a batch of %d rows', $i, $this->count);
        }

        $values = [];

        foreach ($this->columns as $name => $column) {
            $values[$name] = $column->value($i);
        }

        return $values;
    }

    /**
     * @param array<array-key, Column> $columns each of count() rows; a same-named column is replaced in place
     *
     * @throws InvalidArgumentException
     */
    public function withColumns(Schema $schema, array $columns): self
    {
        foreach ($columns as $name => $column) {
            if ($column->count() !== $this->count) {
                throw new InvalidArgumentException(sprintf(
                    'Column "%s" holds %d rows, the batch %d',
                    $name,
                    $column->count(),
                    $this->count,
                ));
            }
        }

        $merged = [];

        foreach ($schema->definitions() as $name => $_) {
            if (array_key_exists($name, $columns)) {
                $merged[$name] = $columns[$name];
            } elseif (array_key_exists($name, $this->columns)) {
                $merged[$name] = $this->columns[$name];
            }
        }

        return self::fromColumns($schema, $merged, $this->count);
    }

    /**
     * Positional restamp: the n-th definition of $schema retypes the n-th column (Column::withType).
     *
     * @throws ColumnMismatchException
     * @throws InvalidArgumentException
     */
    public function withSchema(Schema $schema): self
    {
        if ($schema->count() !== $this->schema->count()) {
            throw new InvalidArgumentException(sprintf(
                'Rows::withSchema() expects %d definitions, got %d',
                $this->schema->count(),
                $schema->count(),
            ));
        }

        $columns = array_values($this->columns);
        $retyped = [];
        $position = 0;

        foreach ($schema->definitions() as $name => $definition) {
            $column = $columns[$position++];

            if (!$definition->isNullable() && $column->nullCount() > 0) {
                throw ColumnMismatchException::valueDoesNotMatch($definition, null);
            }

            try {
                $retyped[$name] = $column->withType($definition->type());
            } catch (InvalidArgumentException $e) {
                throw ColumnMismatchException::retype($definition, $e->getMessage());
            }
        }

        return new self($schema, $retyped, $this->count);
    }

    /**
     * @throws InvalidArgumentException
     */
    private function joinUsing(self $right, Expression $expression, Join $type): self
    {
        $single = static function (self $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner($expression, $type);
        $joined = [];

        foreach ($joiner->join(JoinSide::of($single($this)), JoinSide::of($single($right))) as $batch) {
            $joined[] = $batch;
        }

        return $joined === []
            ? self::empty($joiner->schema($this->schema, $right->schema))
            : $joined[0]->concat(...array_slice($joined, 1));
    }
}
