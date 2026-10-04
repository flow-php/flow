<?php

declare(strict_types=1);

namespace Flow\ETL;

use Countable;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows\ColumnRetyping;
use Flow\ETL\Schema\Formatter\InlineSchemaFormatter;
use Flow\ETL\Schema\SimilarNames;
use Flow\ETL\Sort\RowOrder;
use Flow\Floe\FrameDecoder;
use Flow\Floe\FrameEncoder;
use Flow\Types\Value\Json;
use Generator;

use function array_diff_key;
use function array_fill;
use function array_flip;
use function array_key_exists;
use function array_keys;
use function array_shift;
use function array_values;
use function count;
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
     * A serialized batch carries no context, so it decodes in the default backend.
     *
     * @param RowsPayload $data
     */
    public function __unserialize(array $data): void
    {
        $rows = (new FrameDecoder())->decode($data['frame'], $data['schema'], new AdaptiveBackend());

        $this->schema = $rows->schema;
        $this->columns = $rows->columns;
        $this->count = $rows->count;
    }

    public static function empty(Schema $schema, Backend $backend): self
    {
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

        $retyping = new ColumnRetyping();

        foreach ($schema->definitions() as $name => $definition) {
            if ($columns[$name] instanceof ValueColumn) {
                throw ColumnMismatchException::untypedColumn($definition);
            }

            $retyping->notNull($definition, $columns[$name]);
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
     * The same batch with every column in $backend's storage - itself when $backend already owns every column.
     *
     * @throws ColumnMismatchException a null under NOT NULL on a column that is copied
     */
    public function adoptedBy(Backend $backend): self
    {
        $columns = [];
        $copied = false;

        foreach ($this->schema->definitions() as $name => $definition) {
            $columns[$name] = $backend->adopt($definition, $this->columns[$name]);
            $copied = $copied || $columns[$name] !== $this->columns[$name];
        }

        return $copied ? new self($this->schema, $columns, $this->count) : $this;
    }

    /**
     * Every input carries this schema; zero-row inputs add no rows. The result's columns are $backend's.
     *
     * @throws InvalidArgumentException
     */
    public function concat(Backend $backend, self ...$others): self
    {
        $inputs = $this->count > 0 ? [$this] : [];
        $count = $this->count;

        foreach ($others as $other) {
            if (!$other->schema->isSame($this->schema)) {
                throw InvalidArgumentException::because(
                    'Cannot concat Rows with different schemas: [%s] and [%s]',
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
            return $this->adoptedBy($backend);
        }

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
                $columns[$name] = $backend->adopt($definition, $first->concat(...$rest));

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

    public function isEmpty(): bool
    {
        return $this->count === 0;
    }

    /**
     * Adopts $schema. A changed column whose type change the types prove is restamped; any other change is checked
     * value by value and rebuilt in $backend.
     *
     * @throws SchemaMismatchException
     */
    public function matchTo(Schema $schema, Backend $backend): self
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

        $columns = [];
        $changed = [];
        $retyping = new ColumnRetyping();

        foreach ($schema->definitions() as $name => $definition) {
            $own = $this->schema->findDefinition($definition->entry()->name());

            if ($own === null) {
                $columns[$name] = $retyping->absent($definition, $this->count, $backend);

                continue;
            }

            $restamped = $retyping->restamped($this->columns[$name], $own, $definition);

            if ($restamped === null) {
                $changed[$name] = [$this->columns[$name], $definition];
            }

            $columns[$name] = $restamped;
        }

        $retyping->validate($changed, $this->count);

        foreach ($changed as $name => [$column, $definition]) {
            $columns[$name] = $retyping->rebuilt($column, $definition, $backend);
        }

        /** @var array<string, Column> $columns */
        return self::fromColumns($schema, $columns, $this->count);
    }

    /**
     * Drops the columns $schema does not declare and adopts it, converting a kept column whose definition $schema
     * changes (in $backend). Contrast select(), which keeps the named columns as they are. Widening the batch with
     * columns it lacks is not a projection - that goes through matchTo().
     *
     * @throws SchemaMismatchException
     */
    public function project(Schema $schema, Backend $backend): self
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

        return (new self(new Schema(...$definitions), $columns, $this->count))->matchTo($schema, $backend);
    }

    /**
     * The named columns as they are, in schema order - every column kept unconverted, in whatever backend built it.
     * Contrast project(), which adopts a given schema and converts the columns it changes.
     *
     * @throws SchemaDefinitionNotFoundException
     */
    public function select(string ...$names): self
    {
        $keep = array_flip($names);
        $definitions = [];
        $columns = [];

        foreach ($this->schema->definitions() as $name => $definition) {
            if (array_key_exists($definition->entry()->name(), $keep)) {
                $definitions[] = $definition;
                $columns[$name] = $this->columns[$name];
            }
        }

        if (count($definitions) !== count($keep)) {
            foreach ($names as $selected) {
                if ($this->schema->findDefinition($selected) === null) {
                    throw SchemaDefinitionNotFoundException::withAvailable(
                        $selected,
                        ...$this->schema->references()->names(),
                    );
                }
            }
        }

        return new self(new Schema(...$definitions), $columns, $this->count);
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
    public function sortBy(Reference ...$references): self
    {
        if ($this->count === 0) {
            return $this;
        }

        $order = new RowOrder(array_values($references));

        return $this->gather($order->permutation($order->keys($this), $this->count));
    }

    public function take(int $size): self
    {
        if ($size < 0) {
            throw new InvalidArgumentException('Size must be greater than or equal to 0');
        }

        return $this->slice(0, min($size, $this->count));
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
     * @throws SchemaMismatchException a null under a NOT NULL definition, at its row
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
        $retyping = new ColumnRetyping();

        foreach ($schema->definitions() as $name => $definition) {
            $column = $columns[$position++];
            $retyping->notNull($definition, $column);

            try {
                $retyped[$name] = $column->withType($definition->type());
            } catch (InvalidArgumentException $e) {
                throw ColumnMismatchException::retype($definition, $e->getMessage());
            }
        }

        return new self($schema, $retyped, $this->count);
    }
}
