<?php

declare(strict_types=1);

namespace Flow\ETL;

use ArrayAccess;
use ArrayIterator;
use Countable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Join\Join;
use Flow\ETL\Row\CartesianProduct;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Sort\ValuesSorter;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use Iterator;
use IteratorAggregate;

use function array_filter;
use function array_map;
use function array_merge;
use function array_reduce;
use function array_reverse;
use function array_slice;
use function array_unique;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function is_array;
use function is_int;
use function iterator_to_array;
use function usort;

/**
 * @implements \ArrayAccess<int, Row>
 * @implements \IteratorAggregate<int, Row>
 */
final class Rows implements ArrayAccess, Countable, IteratorAggregate
{
    private Partitions $partitions;

    /**
     * @var array<int, Row>
     */
    private array $rows;

    private ?Schema $schema = null;

    public function __construct(Row ...$rows)
    {
        $this->rows = array_values($rows);
        $this->partitions = new Partitions();
    }

    /**
     * @param array<int, Row>|array<Row> $rows
     * @param array<Partition>|array<string, string>|Partitions $partitions
     */
    public static function partitioned(array $rows, array|Partitions $partitions): self
    {
        if (!count($rows)) {
            return new self();
        }

        if (is_array($partitions)) {
            $allArePartitions =
                count($partitions) > 0
                && array_reduce(
                    $partitions,
                    static fn(bool $carry, $item) => $carry && $item instanceof Partition,
                    true,
                );

            if ($allArePartitions) {
                // All elements are Partition objects, safe to spread
                $partitions = new Partitions(...array_filter(
                    $partitions,
                    static fn($item) => $item instanceof Partition,
                ));
            } else {
                // Convert associative array to Partitions
                /** @var array<string, string> $typedPartitions */
                $typedPartitions = $partitions;
                $partitions = new Partitions(...Partition::fromArray($typedPartitions));
            }
        }

        $rows = new self(...$rows);
        $rows->partitions = $partitions;

        return $rows;
    }

    public function add(Row ...$rows): self
    {
        return new self(...$this->rows, ...$rows);
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
            $rows = new self();
            $rows->rows = $chunk;
            $rows->partitions = $this->partitions;

            yield $rows;
        }
    }

    public function count(): int
    {
        return count($this->rows);
    }

    public function diffLeft(self $rows): self
    {
        $differentRows = [];

        foreach ($this->rows as $row) {
            $found = false;

            foreach ($rows->rows as $otherRow) {
                if ($row->isEqual($otherRow)) {
                    $found = true;

                    break;
                }
            }

            if (!$found) {
                $differentRows[] = $row;
            }
        }

        return self::partitioned($differentRows, $this->partitions);
    }

    public function diffRight(self $rows): self
    {
        $differentRows = [];

        foreach ($rows->rows as $row) {
            $found = false;

            foreach ($this->rows as $otherRow) {
                if ($row->isEqual($otherRow)) {
                    $found = true;

                    break;
                }
            }

            if (!$found) {
                $differentRows[] = $row;
            }
        }

        return self::partitioned($differentRows, $this->partitions);
    }

    public function drop(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::partitioned(array_slice($this->rows, $size), $this->partitions);
    }

    public function dropPartitions(bool $dropPartitionColumns = false): self
    {
        $rows = new self(...$this->rows);

        if ($dropPartitionColumns) {
            return $rows->map(fn(Row $row): Row => $row->remove(...array_map(
                static fn(Partition $partition): Reference => $partition->reference(),
                $this->partitions->toArray(),
            )));
        }

        return $rows;
    }

    public function dropRight(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::partitioned(array_slice($this->rows, 0, -$size), $this->partitions);
    }

    /**
     * @param callable(Row) : void $callable
     */
    public function each(callable $callable): void
    {
        foreach ($this->rows as $row) {
            $callable($row);
        }
    }

    public function empty(): bool
    {
        return $this->count() === 0;
    }

    /**
     * @return array<Entries>
     */
    public function entries(): array
    {
        $entries = [];

        foreach ($this->rows as $row) {
            $entries[] = $row->entries();
        }

        return $entries;
    }

    /**
     * @param callable(Row) : bool $callable
     */
    public function filter(callable $callable): self
    {
        $results = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $results[] = $row;
            }
        }

        return self::partitioned($results, $this->partitions);
    }

    public function find(callable $callable): self
    {
        if (0 === $this->count()) {
            return new self();
        }

        $rows = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $rows[] = $row;
            }
        }

        return self::partitioned($rows, $this->partitions);
    }

    public function findOne(callable $callable): ?Row
    {
        foreach ($this->rows as $row) {
            if ($callable($row)) {
                return $row;
            }
        }

        return null;
    }

    public function first(): Row
    {
        return $this->rows[0] ?? throw new RuntimeException('First row does not exist in empty collection');
    }

    /**
     * @param callable(Row) : array<Row> $callable
     */
    public function flatMap(callable $callable): self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return new self(...array_merge(...$rows));
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
            $hash .= $row->hash($algorithm);
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
            return self::partitioned([], $this->partitions);
        }

        return self::partitioned(array_slice($this->rows, 0, $count), $this->partitions);
    }

    public function isPartitioned(): bool
    {
        return count($this->partitions) > 0;
    }

    public function joinCross(self $right, string $joinPrefix = 'joined_'): self
    {
        /**
         * @var array<Row> $joined
         */
        $joined = [];

        if ($right->count() === 0) {
            return $this;
        }

        if ($this->count() === 0) {
            return $right;
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

        return new self(...$joined);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinInner(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::inner, new EntryFactory());
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeft(self $right, Expression $expression, EntryFactory $entryFactory): self
    {
        return $this->joinUsing($right, $expression, Join::left, $entryFactory);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeftAnti(self $right, Expression $expression): self
    {
        return $this->joinUsing($right, $expression, Join::left_anti, new EntryFactory());
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinRight(self $right, Expression $expression, EntryFactory $entryFactory): self
    {
        return $this->joinUsing($right, $expression, Join::right, $entryFactory);
    }

    /**
     * @throws InvalidArgumentException
     */
    private function joinUsing(self $right, Expression $expression, Join $type, EntryFactory $entryFactory): self
    {
        $single = static function (self $rows): Generator {
            yield $rows;
        };

        /**
         * @var array<Row> $joined
         */
        $joined = [];

        foreach ((new Joiner($expression, $type, $entryFactory))->join($single($this), $single($right)) as $batch) {
            foreach ($batch as $row) {
                $joined[] = $row;
            }
        }

        return new self(...$joined);
    }

    public function last(): ?Row
    {
        if (empty($this->rows)) {
            return null;
        }

        return $this->rows[count($this->rows) - 1];
    }

    /**
     * @param callable(Row) : Row $callable
     */
    public function map(callable $callable): self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return self::partitioned($rows, $this->partitions);
    }

    public function merge(self $rows): self
    {
        if ($this->empty()) {
            return $rows;
        }

        if ($rows->empty()) {
            return $this;
        }

        if ($this->partitions->id() === $rows->partitions()->id()) {
            $mergedRows = new self(...$this->rows, ...$rows->rows);
            $mergedRows->partitions = $this->partitions;

            return $mergedRows;
        }

        return new self(...$this->rows, ...$rows->rows);
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
    public function partitionBy(string|Reference $reference, string|Reference ...$references): array
    {
        $refs = References::init($reference, ...$references);

        /** @var array<string, array<mixed>> $partitions */
        $partitions = [];

        foreach ($refs as $ref) {
            foreach ($this->rows as $row) {
                $partitions[$ref->name()][] = Partition::valueFromRow($ref, $row);
            }

            $partitions[$ref->name()] = array_values(array_unique($partitions[$ref->name()]));
        }

        /** @var array<Rows> $partitionedRows */
        $partitionedRows = [];

        /**
         * @var array<string, mixed> $partitionsData
         */
        foreach ((new CartesianProduct())($partitions) as $partitionsData) {
            $parts = Partition::fromArray($partitionsData);
            $rows = [];

            foreach ($this->rows as $row) {
                foreach ($parts as $partition) {
                    if (Partition::valueFromRow($partition->reference(), $row) !== $partition->value) {
                        continue 2;
                    }
                }

                $rows[] = $row;
            }

            if ($rows) {
                $partitionedRows[] = self::partitioned($rows, $parts);
            }
        }

        return $partitionedRows;
    }

    public function partitions(): Partitions
    {
        return $this->partitions;
    }

    /**
     * @param callable(mixed, Row) : mixed $callable
     * @param null|mixed $input
     *
     * @return null|mixed
     */
    public function reduce(callable $callable, mixed $input = null)
    {
        return array_reduce($this->rows, $callable, $input);
    }

    /**
     * @return array<mixed>
     */
    public function reduceToArray(string|Reference $reference): array
    {
        // @mago-ignore analysis:mixed-assignment
        $result = $this->reduce(static function (mixed $ids, Row $row) use ($reference): array {
            if (!is_array($ids)) {
                $ids = [];
            }
            $ids[] = $row->valueOf($reference);

            return $ids;
        }, []);

        return is_array($result) ? $result : [];
    }

    public function remove(int $offset): self
    {
        if (!$this->offsetExists($offset)) {
            throw new InvalidArgumentException("Rows does not have {$offset} offset");
        }

        $rows = iterator_to_array($this->getIterator());
        unset($rows[$offset]);

        return self::partitioned($rows, $this->partitions);
    }

    public function reverse(): self
    {
        return self::partitioned(array_reverse($this->rows), $this->partitions);
    }

    /**
     * @return Schema
     */
    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        if (!$this->count()) {
            return new Schema();
        }

        /** @var ?Schema $schema */
        $schema = null;

        foreach ($this->rows as $row) {
            if ($schema === null) {
                $schema = $row->schema();
            } else {
                $schema = $schema->merge($row->schema());
            }
        }

        /** @var Schema $schema */
        $this->schema = $schema;

        return $this->schema;
    }

    /**
     * @param callable(mixed, mixed) : int $callback
     */
    public function sort(callable $callback): self
    {
        $rows = $this->rows;
        usort($rows, $callback);

        return self::partitioned($rows, $this->partitions);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function sortAscending(string|Reference $reference): self
    {
        $values = [];

        foreach ($this->rows as $index => $row) {
            $values[$index] = $row->valueOf($reference);
        }

        $rows = [];

        foreach (array_keys(ValuesSorter::sort($values, SortOrder::ASC)) as $index) {
            $rows[] = $this->rows[$index];
        }

        return self::partitioned($rows, $this->partitions);
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
            $values[$index] = $row->valueOf($reference);
        }

        $rows = [];

        foreach (array_keys(ValuesSorter::sort($values, SortOrder::DESC)) as $index) {
            $rows[] = $this->rows[$index];
        }

        return self::partitioned($rows, $this->partitions);
    }

    public function sortEntries(): self
    {
        return $this->map(static fn(Row $row): Row => $row->sortEntries());
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
            return self::partitioned([], $this->partitions);
        }

        $rowsCount = count($this->rows);

        if ($count >= $rowsCount) {
            return self::partitioned($this->rows, $this->partitions);
        }

        return self::partitioned(array_slice($this->rows, -$count), $this->partitions);
    }

    public function take(int $size): self
    {
        return self::partitioned(array_slice($this->rows, 0, $size), $this->partitions);
    }

    public function takeRight(int $size): self
    {
        return self::partitioned(array_reverse(array_slice($this->rows, -$size, $size)), $this->partitions);
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
                if ($comparator->equals($row, $uniqueRow)) {
                    $alreadyAdded = true;

                    break;
                }
            }

            if (!$alreadyAdded) {
                $uniqueRows[] = $row;
            }
        }

        return self::partitioned($uniqueRows, $this->partitions);
    }
}
