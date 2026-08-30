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
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Row\CartesianProduct;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SortingStrategy;
use Flow\ETL\Schema\SortingStrategy\AlphabeticalStrategy;
use Flow\ETL\Serializer\DomValueCodec;
use Flow\ETL\Sort\ValuesSorter;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use Iterator;
use IteratorAggregate;

use function array_filter;
use function array_key_exists;
use function array_map;
use function array_merge;
use function array_reduce;
use function array_reverse;
use function array_slice;
use function array_unique;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function implode;
use function is_array;
use function is_int;
use function iterator_to_array;
use function sprintf;
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

    public function __construct(
        private Schema $schema,
        Row ...$rows,
    ) {
        $this->rows = array_values($rows);
        $this->partitions = new Partitions();
    }

    /**
     * @param array<int, Row>|array<Row> $rows
     * @param array<Partition>|array<string, string>|Partitions $partitions
     */
    public static function partitioned(Schema $schema, array $rows, array|Partitions $partitions): self
    {
        if (!count($rows)) {
            return new self($schema);
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

        $rows = new self($schema, ...$rows);
        $rows->partitions = $partitions;

        return $rows;
    }

    /**
     * @return array{schema: Schema, partitions: Partitions, rows: list<array<string, mixed>>}
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

        return ['schema' => $this->schema, 'partitions' => $this->partitions, 'rows' => $rows];
    }

    /**
     * @param array{schema: Schema, partitions: Partitions, rows: list<array<string, mixed>>} $data
     */
    public function __unserialize(array $data): void
    {
        $this->schema = $data['schema'];
        $this->partitions = $data['partitions'];

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

        $this->rows = $rows;
    }

    public function add(Row ...$rows): self
    {
        return new self($this->schema, ...$this->rows, ...$rows);
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
            $rows = new self($this->schema);
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

        return self::partitioned($this->schema, $differentRows, $this->partitions);
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

        return self::partitioned($this->schema, $differentRows, $this->partitions);
    }

    public function drop(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::partitioned($this->schema, array_slice($this->rows, $size), $this->partitions);
    }

    public function dropPartitions(bool $dropPartitionColumns = false): self
    {
        if ($dropPartitionColumns) {
            return new self(
                $this->schema->gracefulRemove(...array_map(
                    static fn(Partition $partition): string => $partition->name,
                    $this->partitions->toArray(),
                )),
                ...$this->rows,
            );
        }

        return new self($this->schema, ...$this->rows);
    }

    public function dropRight(int $size): self
    {
        if ($size === 0) {
            return $this;
        }

        return self::partitioned($this->schema, array_slice($this->rows, 0, -$size), $this->partitions);
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

        return self::partitioned($this->schema, $results, $this->partitions);
    }

    public function find(callable $callable): self
    {
        if (0 === $this->count()) {
            return new self($this->schema);
        }

        $rows = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $rows[] = $row;
            }
        }

        return self::partitioned($this->schema, $rows, $this->partitions);
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
    public function flatMap(Schema $schema, callable $callable): self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return new self($schema, ...array_merge(...$rows));
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
            return self::partitioned($this->schema, [], $this->partitions);
        }

        return self::partitioned($this->schema, array_slice($this->rows, 0, $count), $this->partitions);
    }

    public function isPartitioned(): bool
    {
        return count($this->partitions) > 0;
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
            return new self($this->schema, ...$this->rows);
        }

        if ($this->count() === 0) {
            return new self($right->schema, ...$right->rows);
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

        foreach ($joiner->join($single($this), $single($right)) as $batch) {
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

    /**
     * @param callable(Row) : Row $callable
     */
    public function map(Schema $schema, callable $callable): self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return self::partitioned($schema, $rows, $this->partitions);
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

        if ($this->partitions->id() === $rows->partitions()->id()) {
            $mergedRows = new self($this->schema, ...$this->rows, ...$rows->rows);
            $mergedRows->partitions = $this->partitions;

            return $mergedRows;
        }

        return new self($this->schema, ...$this->rows, ...$rows->rows);
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
                $partitions[$ref->name()][] = Partition::fromValue(
                    $ref->name(),
                    $this->schema->get($ref)->type(),
                    $row->get($ref),
                );
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
                    if (
                        Partition::fromValue(
                            $partition->name,
                            $this->schema->get($partition->name)->type(),
                            $row->get($partition->name),
                        ) !== $partition->value
                    ) {
                        continue 2;
                    }
                }

                $rows[] = $row;
            }

            if ($rows) {
                $partitionedRows[] = self::partitioned($this->schema, $rows, $parts);
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
            $ids[] = $row->get($reference);

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

        return self::partitioned($this->schema, $rows, $this->partitions);
    }

    public function reverse(): self
    {
        return self::partitioned($this->schema, array_reverse($this->rows), $this->partitions);
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    /**
     * @param callable(mixed, mixed) : int $callback
     */
    public function sort(callable $callback): self
    {
        $rows = $this->rows;
        usort($rows, $callback);

        return self::partitioned($this->schema, $rows, $this->partitions);
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

        return self::partitioned($this->schema, $rows, $this->partitions);
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

        return self::partitioned($this->schema, $rows, $this->partitions);
    }

    public function sortEntries(SortingStrategy $strategy = new AlphabeticalStrategy()): self
    {
        return new self($this->schema->sort($strategy), ...$this->rows);
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
            return self::partitioned($this->schema, [], $this->partitions);
        }

        $rowsCount = count($this->rows);

        if ($count >= $rowsCount) {
            return self::partitioned($this->schema, $this->rows, $this->partitions);
        }

        return self::partitioned($this->schema, array_slice($this->rows, -$count), $this->partitions);
    }

    public function take(int $size): self
    {
        return self::partitioned($this->schema, array_slice($this->rows, 0, $size), $this->partitions);
    }

    public function takeRight(int $size): self
    {
        return self::partitioned(
            $this->schema,
            array_reverse(array_slice($this->rows, -$size, $size)),
            $this->partitions,
        );
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

        return self::partitioned($this->schema, $uniqueRows, $this->partitions);
    }
}
