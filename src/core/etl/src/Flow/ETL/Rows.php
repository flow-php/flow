<?php

declare(strict_types=1);

namespace Flow\ETL;

use ArrayAccess;
use ArrayIterator;
use Countable;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\JoinSide;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Serializer\DomValueCodec;
use Flow\ETL\Sort\ValuesSorter;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use Iterator;
use IteratorAggregate;

use function array_key_exists;
use function array_map;
use function array_reverse;
use function array_slice;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function implode;
use function is_int;
use function iterator_to_array;
use function sprintf;

/**
 * @implements \ArrayAccess<int, Row>
 * @implements \IteratorAggregate<int, Row>
 */
final class Rows implements ArrayAccess, Countable, IteratorAggregate
{
    /**
     * @var array<int, Row>
     */
    private array $rows;

    /**
     * @throws SchemaMismatchException
     */
    public function __construct(
        private Schema $schema,
        Row ...$rows,
    ) {
        $this->rows = [];

        foreach ($rows as $row) {
            try {
                $this->rows[] = $row->matchTo($schema);
            } catch (ColumnMismatchException $e) {
                throw new SchemaMismatchException(count($this->rows), $e);
            }
        }
    }

    /**
     * Skips the shape check the constructor performs. The caller vouches that every row already
     * satisfies $schema and stores its columns in the Schema's order - which holds when the same
     * operation produced the schema and the rows, or when the rows are a subset or a permutation of
     * a batch that already passed.
     *
     * @internal engine paths only
     *
     * @param array<int, Row> $rows re-indexed here - first(), last(), chunks() and offsetGet() read by position
     */
    public static function trusted(Schema $schema, array $rows): self
    {
        $instance = new self($schema);
        $instance->rows = array_values($rows);

        return $instance;
    }

    /**
     * @return array{schema: Schema, rows: list<array<string, mixed>>}
     */
    public function __serialize(): array
    {
        $codec = new DomValueCodec();
        $domColumns = [];

        foreach ($this->schema->definitions() as $definition) {
            if ($codec->handles($definition->type())) {
                $domColumns[$definition->entry()->name()] = $definition->type();
            }
        }

        $rows = [];

        foreach ($this->rows as $row) {
            $values = $row->values();

            foreach ($domColumns as $name => $type) {
                if (array_key_exists($name, $values)) {
                    $values[$name] = $codec->encode($type, $values[$name]);
                }
            }

            $rows[] = $values;
        }

        return ['schema' => $this->schema, 'rows' => $rows];
    }

    /**
     * @param array{schema: Schema, rows: list<array<string, mixed>>} $data
     */
    public function __unserialize(array $data): void
    {
        $this->schema = $data['schema'];

        $codec = new DomValueCodec();
        $domColumns = [];

        foreach ($this->schema->definitions() as $definition) {
            if ($codec->handles($definition->type())) {
                $domColumns[$definition->entry()->name()] = $definition->type();
            }
        }

        $rows = [];

        foreach ($data['rows'] as $values) {
            foreach ($domColumns as $name => $type) {
                if (array_key_exists($name, $values)) {
                    $values[$name] = $codec->decode($type, $values[$name]);
                }
            }

            $rows[] = new Row($values);
        }

        $this->rows = (new self($this->schema, ...$rows))->rows;
    }

    /**
     * @throws SchemaMismatchException
     */
    public function add(Row ...$rows): self
    {
        $matched = $this->rows;

        foreach ($rows as $row) {
            try {
                $matched[] = $row->matchTo($this->schema);
            } catch (ColumnMismatchException $e) {
                throw new SchemaMismatchException(count($matched), $e);
            }
        }

        return self::trusted($this->schema, $matched);
    }

    /**
     * @return array<Row>
     */
    public function all(): array
    {
        return $this->rows;
    }

    /**
     * @param int<1, max> $size
     *
     * @return \Generator<Rows>
     */
    public function chunks(int $size): Generator
    {
        foreach (array_chunk($this->rows, $size) as $chunk) {
            yield self::trusted($this->schema, $chunk);
        }
    }

    /**
     * Re-checks the batch against a different Schema and adopts it - the door the constructor opens,
     * for rows already gathered into a batch.
     *
     * @throws SchemaMismatchException
     */
    public function matchTo(Schema $schema): self
    {
        return new self($schema, ...$this->rows);
    }

    public function count(): int
    {
        return count($this->rows);
    }

    public function diffLeft(self $rows): self
    {
        $comparator = new NativeComparator();
        $differentRows = [];

        foreach ($this->rows as $row) {
            $found = false;

            foreach ($rows->rows as $otherRow) {
                if ($comparator->equals($row, $otherRow, $this->schema)) {
                    $found = true;

                    break;
                }
            }

            if (!$found) {
                $differentRows[] = $row;
            }
        }

        return self::trusted($this->schema, $differentRows);
    }

    public function diffRight(self $rows): self
    {
        $comparator = new NativeComparator();
        $differentRows = [];

        foreach ($rows->rows as $row) {
            $found = false;

            foreach ($this->rows as $otherRow) {
                if ($comparator->equals($row, $otherRow, $this->schema)) {
                    $found = true;

                    break;
                }
            }

            if (!$found) {
                $differentRows[] = $row;
            }
        }

        // the surviving rows come from the right side, so the right side's schema describes them
        return self::trusted($rows->schema, $differentRows);
    }

    public function drop(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::trusted($this->schema, array_slice($this->rows, $size));
    }

    public function dropRight(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::trusted($this->schema, array_slice($this->rows, 0, -$size));
    }

    public function empty(): bool
    {
        return $this->count() === 0;
    }

    public function first(): Row
    {
        return $this->rows[0] ?? throw new RuntimeException('First row does not exist in empty collection');
    }

    /**
     * @return \Iterator<int, Row>
     */
    public function getIterator(): Iterator
    {
        return new ArrayIterator($this->rows);
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()): string
    {
        $hash = '';

        foreach ($this->rows as $row) {
            $hash .= $row->hash($this->schema, $algorithm);
        }

        return $algorithm->hash($hash);
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

        if ($count === 0) {
            return self::trusted($this->schema, []);
        }

        return self::trusted($this->schema, array_slice($this->rows, 0, $count));
    }

    public function joinCross(self $right, string $joinPrefix = 'joined_'): self
    {
        $schema = (new JoinSchema($joinPrefix))->cross($this->schema, $right->schema);

        /**
         * @var array<Row> $joined
         */
        $joined = [];

        // nothing was merged, so the surviving side keeps its own schema - pairing the cross schema
        // with unjoined rows would hand back a batch whose schema does not describe its rows
        if ($right->count() === 0) {
            return self::trusted($this->schema, $this->rows);
        }

        if ($this->count() === 0) {
            return self::trusted($right->schema, $right->rows);
        }

        $merger = new RowMerger($joinPrefix);

        foreach ($this->rows as $leftRow) {
            foreach ($right->rows as $rightRow) {
                try {
                    $joined[] = $merger->merge($leftRow, $rightRow);
                } catch (InvalidArgumentException $e) {
                    throw new InvalidArgumentException($e->getMessage() . '. Please consider using join prefix option');
                }
            }
        }

        return new self($schema, ...$joined);
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
     * @throws InvalidArgumentException
     */
    private function joinUsing(self $right, Expression $expression, Join $type): self
    {
        $single = static function (self $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner($expression, $type);

        /**
         * @var array<Row> $joined
         */
        $joined = [];

        foreach ($joiner->join(JoinSide::of($single($this)), JoinSide::of($single($right))) as $batch) {
            foreach ($batch as $row) {
                $joined[] = $row;
            }
        }

        return new self($joiner->schema($this->schema, $right->schema), ...$joined);
    }

    public function last(): ?Row
    {
        if (empty($this->rows)) {
            return null;
        }

        return $this->rows[count($this->rows) - 1];
    }

    public function merge(self $rows): self
    {
        if ($this->empty()) {
            return $rows;
        }

        if ($rows->empty()) {
            return $this;
        }

        if (!$this->schema->isSame($rows->schema())) {
            // names alone cannot show a type-only disagreement - both sides would print identically
            $describe = static fn(Schema $schema): string => implode(', ', array_map(
                static fn(Definition $definition): string => (
                    $definition->entry()->name()
                    . ': '
                    . ($definition->isNullable() ? '?' : '')
                    . $definition->type()->toString()
                ),
                $schema->definitions(),
            ));

            throw new InvalidArgumentException(sprintf(
                'Cannot merge Rows with different schemas: [%s] and [%s]',
                $describe($this->schema),
                $describe($rows->schema()),
            ));
        }

        return self::trusted($this->schema, [...$this->rows, ...$rows->rows]);
    }

    /**
     * @param int $offset
     *
     * @throws InvalidArgumentException
     */
    public function offsetExists($offset): bool
    {
        // @mago-ignore analysis:impossible-condition,redundant-type-comparison
        if (!is_int($offset)) {
            throw new InvalidArgumentException('Rows accepts only integer offsets');
        }

        return isset($this->rows[$offset]);
    }

    /**
     * @param int $offset
     *
     * @throws InvalidArgumentException
     */
    public function offsetGet($offset): Row
    {
        if ($this->offsetExists($offset)) {
            return $this->rows[$offset];
        }

        throw new InvalidArgumentException("Row {$offset} does not exists.");
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        throw new RuntimeException('In order to add new rows use Rows::add(Row $row) : self');
    }

    /**
     * @param int $offset
     *
     * @throws RuntimeException
     */
    public function offsetUnset(mixed $offset): void
    {
        throw new RuntimeException('In order to remove rows use Rows::remove(int $offset) : self');
    }

    /**
     * @param Reference|string $reference
     * @param Reference|string ...$references
     *
     * @throws InvalidArgumentException
     *
     * @return array<Rows>
     */
    /**
     * @return array<mixed>
     */
    /**
     * Drops the columns $schema does not declare and adopts it. Widening or retyping the batch is
     * not a projection - that goes through matchTo().
     *
     * @throws SchemaMismatchException
     */
    public function project(Schema $schema): self
    {
        $projected = [];

        foreach ($this->rows as $row) {
            $projected[] = $row->project($schema);
        }

        return new self($schema, ...$projected);
    }

    public function reduceToArray(string|Reference $reference): array
    {
        $ids = [];

        foreach ($this->rows as $row) {
            $ids[] = $row->get($reference);
        }

        return $ids;
    }

    public function remove(int $offset): self
    {
        if (!$this->offsetExists($offset)) {
            throw new InvalidArgumentException("Rows does not have {$offset} offset");
        }

        $rows = iterator_to_array($this->getIterator());
        unset($rows[$offset]);

        return self::trusted($this->schema, array_values($rows));
    }

    public function reverse(): self
    {
        return self::trusted($this->schema, array_reverse($this->rows));
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortAscending(string|Reference $reference): self
    {
        $values = [];

        foreach ($this->rows as $index => $row) {
            $values[$index] = $row->get($reference);
        }

        $rows = [];

        foreach (array_keys(ValuesSorter::sort($values, SortOrder::ASC)) as $index) {
            $rows[] = $this->rows[$index];
        }

        return self::trusted($this->schema, $rows);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortBy(Reference ...$references): self
    {
        $rows = $this;

        foreach (array_reverse($references) as $ref) {
            $rows = $ref->sort() === SortOrder::ASC ? $rows->sortAscending($ref) : $rows->sortDescending($ref);
        }

        return $rows;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortDescending(string|Reference $reference): self
    {
        $values = [];

        foreach ($this->rows as $index => $row) {
            $values[$index] = $row->get($reference);
        }

        $rows = [];

        foreach (array_keys(ValuesSorter::sort($values, SortOrder::DESC)) as $index) {
            $rows[] = $this->rows[$index];
        }

        return self::trusted($this->schema, $rows);
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

        if ($count === 0) {
            return self::trusted($this->schema, []);
        }

        $rowsCount = count($this->rows);

        if ($count >= $rowsCount) {
            return self::trusted($this->schema, $this->rows);
        }

        return self::trusted($this->schema, array_slice($this->rows, -$count));
    }

    public function take(int $size): self
    {
        return self::trusted($this->schema, array_slice($this->rows, 0, $size));
    }

    public function takeRight(int $size): self
    {
        return self::trusted($this->schema, array_reverse(array_slice($this->rows, -$size, $size)));
    }

    /**
     * @return array<int, array<array-key, mixed>>
     */
    public function toArray(bool $withKeys = true): array
    {
        $array = [];

        foreach ($this->rows as $row) {
            $array[] = $row->toArray($withKeys);
        }

        return $array;
    }

    public function unique(Comparator $comparator = new NativeComparator()): self
    {
        /**
         * @var array<Row> $uniqueRows
         */
        $uniqueRows = [];

        foreach ($this->rows as $row) {
            $alreadyAdded = false;

            foreach ($uniqueRows as $uniqueRow) {
                if ($comparator->equals($row, $uniqueRow, $this->schema)) {
                    $alreadyAdded = true;

                    break;
                }
            }

            if (!$alreadyAdded) {
                $uniqueRows[] = $row;
            }
        }

        return self::trusted($this->schema, array_values($uniqueRows));
    }
}
