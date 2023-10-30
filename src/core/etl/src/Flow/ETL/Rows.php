<?php

declare(strict_types=1);

namespace Flow\ETL;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\SortOrder;
use Flow\Serializer\Serializable;

/**
 * @implements \ArrayAccess<int, Row>
 * @implements \IteratorAggregate<int, Row>
 * @implements Serializable<array{rows: array<int, Row>}>
 */
final class Rows implements \ArrayAccess, \Countable, \IteratorAggregate, Serializable
{
    /**
     * @var array<int, Row>
     */
    private readonly array $rows;

    public function __construct(Row ...$rows)
    {
        $this->rows = \array_values($rows);
    }

    public static function fromArray(array $data, EntryFactory $entryFactory = new NativeEntryFactory()) : self
    {
        return array_to_rows($data, $entryFactory);
    }

    public function __serialize() : array
    {
        return [
            'rows' => $this->rows,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->rows = $data['rows'];
    }

    public function add(Row ...$rows) : self
    {
        return new self(
            ...$this->rows,
            ...$rows
        );
    }

    /**
     * @param int<1, max> $size
     *
     * @return \Generator<Rows>
     */
    public function chunks(int $size) : \Generator
    {
        foreach (\array_chunk($this->rows, $size) as $chunk) {
            yield new self(...$chunk);
        }
    }

    public function count() : int
    {
        return \count($this->rows);
    }

    public function diffLeft(self $rows) : self
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

        return new self(...$differentRows);
    }

    public function diffRight(self $rows) : self
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

        return new self(...$differentRows);
    }

    public function drop(int $size) : self
    {
        return new self(...\array_slice($this->rows, $size));
    }

    public function dropRight(int $size) : self
    {
        $rows = $this->rows;

        for ($i = 0; $i < $size; $i++) {
            \array_pop($rows);
        }

        return new self(...$rows);
    }

    /**
     * @param callable(Row) : void $callable
     */
    public function each(callable $callable) : void
    {
        foreach ($this->rows as $row) {
            $callable($row);
        }
    }

    public function empty() : bool
    {
        return $this->count() === 0;
    }

    /**
     * @return array<Entries>
     */
    public function entries() : array
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
    public function filter(callable $callable) : self
    {
        $results = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $results[] = $row;
            }
        }

        return new self(...$results);
    }

    public function find(callable $callable) : self
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

        return new self(...$rows);
    }

    public function findOne(callable $callable) : ?Row
    {
        foreach ($this->rows as $row) {
            if ($callable($row)) {
                return $row;
            }
        }

        return null;
    }

    public function first() : Row
    {
        return $this->rows[0] ?? throw new RuntimeException('First row does not exist in empty collection');
    }

    /**
     * @param callable(Row) : Row[] $callable
     */
    public function flatMap(callable $callable) : self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return new self(...\array_merge(...$rows));
    }

    /**
     * @return \Iterator<int, Row>
     */
    public function getIterator() : \Iterator
    {
        return new \ArrayIterator($this->rows);
    }

    public function joinCross(self $right, string $joinPrefix = '') : self
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

        foreach ($this->rows as $leftRow) {
            foreach ($right->rows as $rightRow) {
                try {
                    $joined[] = $leftRow->merge($rightRow, $joinPrefix);
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
    public function joinInner(self $right, Expression $expression) : self
    {
        /**
         * @var array<Row> $joined
         */
        $joined = [];

        foreach ($this->rows as $leftRow) {
            /** @var ?Row $joinedRow */
            $joinedRow = null;

            foreach ($right as $rightRow) {
                if ($expression->meet($leftRow, $rightRow)) {
                    try {
                        $joinedRow = $leftRow
                            ->merge($rightRow, $expression->prefix())
                            ->remove(...\array_map(static fn (EntryReference $e) : string => $expression->prefix() . $e->name(), $expression->right()));
                    } catch (InvalidArgumentException $e) {
                        throw new InvalidArgumentException($e->getMessage() . '. Please consider using Condition, join prefix option');
                    }

                    break;
                }
            }

            if ($joinedRow) {
                $joined[] = $joinedRow;
            }
        }

        return new self(...$joined);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeft(self $right, Expression $expression) : self
    {
        /**
         * @var array<Row> $joined
         */
        $joined = [];

        $rightSchema = $right->schema()->without(...$expression->right());

        foreach ($this->rows as $leftRow) {
            /** @var ?Row $joinedRow */
            $joinedRow = null;

            foreach ($right as $rightRow) {
                if ($expression->meet($leftRow, $rightRow)) {
                    try {
                        $joinedRow = $leftRow
                            ->merge($rightRow, $expression->prefix())
                            ->remove(...\array_map(static fn (EntryReference $e) : string => $expression->prefix() . $e->name(), $expression->right()));
                    } catch (InvalidArgumentException $e) {
                        throw new InvalidArgumentException($e->getMessage() . '. Please consider using Condition, join prefix option');
                    }

                    break;
                }
            }

            $joined[] = $joinedRow ?: $leftRow->merge(
                Row::create(
                    ...\array_map(
                        static fn (string $e) : NullEntry => Entry::null($e),
                        \array_map(
                            static fn (EntryReference $r) : string => $r->name(),
                            $rightSchema->entries()
                        )
                    )
                ),
                $expression->prefix()
            );
        }

        return new self(...$joined);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinLeftAnti(self $right, Expression $expression) : self
    {
        /**
         * @var array<Row> $joined
         */
        $joined = [];

        foreach ($this->rows as $leftRow) {
            $foundRight = false;

            foreach ($right as $rightRow) {
                if (!$expression->meet($leftRow, $rightRow)) {
                    continue;
                }
                $foundRight = true;
            }

            if (!$foundRight) {
                $joined[] = $leftRow;
            }
        }

        return new self(...$joined);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function joinRight(self $right, Expression $expression) : self
    {
        /**
         * @var array<Row> $joined
         */
        $joined = [];

        $leftSchema = $this->schema()->without(...$expression->left());

        foreach ($right->rows as $rightRow) {
            /** @var ?Row $joinedRow */
            $joinedRow = null;

            foreach ($this->rows as $leftRow) {
                if ($expression->meet($leftRow, $rightRow)) {
                    try {
                        $joinedRow = $rightRow
                            ->merge($leftRow, $expression->prefix())
                            ->remove(
                                ...\array_map(static fn (EntryReference $e) : string => $expression->prefix() . $e->name(), $expression->left())
                            );
                    } catch (InvalidArgumentException $e) {
                        throw new InvalidArgumentException($e->getMessage() . '. Please consider using Condition, join prefix option');
                    }

                    $joined[] = $joinedRow;
                }
            }

            if ($joinedRow === null) {
                $joined[] = $rightRow->merge(
                    Row::create(
                        ...\array_map(static fn (EntryReference $e) : NullEntry => Entry::null($e->name()), $leftSchema->entries())
                    ),
                    $expression->prefix()
                );
            }
        }

        return new self(...$joined);
    }

    /**
     * @param callable(Row) : Row $callable
     */
    public function map(callable $callable) : self
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $callable($row);
        }

        return new self(...$rows);
    }

    public function merge(self ...$rows) : self
    {
        $rowsOfRows = [];

        foreach ($rows as $nextRows) {
            $rowsOfRows[] = $nextRows->rows;
        }

        return new self(
            ...\array_merge($this->rows, ...$rowsOfRows)
        );
    }

    /**
     * @param int $offset
     *
     * @throws InvalidArgumentException
     */
    public function offsetExists($offset) : bool
    {
        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_int($offset)) {
            throw new InvalidArgumentException('Rows accepts only integer offsets');
        }

        return isset($this->rows[$offset]);
    }

    /**
     * @param int $offset
     *
     * @throws InvalidArgumentException
     */
    public function offsetGet($offset) : Row
    {
        if ($this->offsetExists($offset)) {
            return $this->rows[$offset];
        }

        throw new InvalidArgumentException("Row {$offset} does not exists.");
    }

    public function offsetSet(mixed $offset, mixed $value) : void
    {
        throw new RuntimeException('In order to add new rows use Rows::add(Row $row) : self');
    }

    /**
     * @param int $offset
     *
     * @throws RuntimeException
     */
    public function offsetUnset(mixed $offset) : void
    {
        throw new RuntimeException('In order to remove rows use Rows::remove(int $offset) : self');
    }

    /**
     * @param Reference|string $entry
     * @param Reference|string ...$entries
     *
     * @throws InvalidArgumentException
     *
     * @return array<PartitionedRows>
     */
    public function partitionBy(string|Reference $entry, string|Reference ...$entries) : array
    {
        $refs = References::init($entry, ...$entries);

        /** @var array<string, array<mixed>> $partitions */
        $partitions = [];

        foreach ($refs as $ref) {
            $partitionEntryValues = [];

            foreach ($this->rows as $row) {
                $partitionEntryValues[$row->get($ref)->value()] = true;
            }

            $partitions[$ref->name()] = \array_keys($partitionEntryValues);
        }

        /**
         * @source https://stackoverflow.com/a/15973172
         *
         * @param array<string, array<mixed>> $input
         *
         * @return array<string, array<mixed>>
         */
        $cartesianProduct = static function (array $input) : array {
            $result = [[]];

            foreach ($input as $key => $values) {
                $append = [];

                foreach ($result as $product) {
                    foreach ($values as $item) {
                        $product[$key] = $item;
                        $append[] = $product;
                    }
                }

                $result = $append;
            }

            return $result;
        };

        /** @var array<PartitionedRows> $partitionedRows */
        $partitionedRows = [];

        /**
         * @var array<string, mixed> $partitionsData
         */
        foreach ($cartesianProduct($partitions) as $partitionsData) {
            $rows = $this->filter(function (Row $row) use ($partitionsData) : bool {
                /**
                 * @var mixed $value
                 */
                foreach ($partitionsData as $entry => $value) {
                    if ($row->valueOf($entry) !== $value) {
                        return false;
                    }
                }

                return true;
            });

            if ($rows->count()) {
                $partitionedRows[] = new PartitionedRows($rows, ...Partition::fromArray($partitionsData));
            }
        }

        return $partitionedRows;
    }

    /**
     * @param callable(mixed, Row) : mixed $callable
     * @param null|mixed $input
     *
     * @return null|mixed
     */
    public function reduce(callable $callable, mixed $input = null)
    {
        return \array_reduce($this->rows, $callable, $input);
    }

    /**
     * @psalm-suppress NullableReturnStatement
     *
     * @return mixed[]
     */
    public function reduceToArray(string|EntryReference $ref) : array
    {
        return $this->reduce(
            function (array $ids, Row $row) use ($ref) : array {
                $ids[] = $row->get($ref)->value();

                return $ids;
            },
            []
        );
    }

    public function remove(int $offset) : self
    {
        if (!$this->offsetExists($offset)) {
            throw new InvalidArgumentException("Rows does not have {$offset} offset");
        }

        $rows = \iterator_to_array($this->getIterator());
        unset($rows[$offset]);

        return new self(...[...$rows]);
    }

    public function reverse() : self
    {
        return new self(...\array_reverse($this->rows));
    }

    public function schema() : Schema
    {
        if (!$this->count()) {
            return new Schema();
        }

        /** @var ?Schema $schema */
        $schema = null;

        foreach ($this->rows as $row) {
            $schema = $schema === null
                ? $row->schema()
                : $schema->merge($row->schema());
        }

        /** @var Schema $schema */
        return $schema;
    }

    /**
     * @param callable(mixed, mixed) : int $callback
     */
    public function sort(callable $callback) : self
    {
        $rows = $this->rows;
        \usort($rows, $callback);

        return new self(...$rows);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortAscending(string|EntryReference $ref) : self
    {
        $rows = $this->rows;
        \usort($rows, fn (Row $a, Row $b) : int => $a->valueOf($ref) <=> $b->valueOf($ref));

        return new self(...$rows);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortBy(EntryReference ...$refs) : self
    {
        $sortBy = References::init(...$refs)->reverse();

        $rows = $this;

        foreach ($sortBy->all() as $ref) {
            $rows = $ref->sort() === SortOrder::ASC ? $rows->sortAscending($ref) : $rows->sortDescending($ref);
        }

        return $rows;
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortDescending(string|EntryReference $ref) : self
    {
        $rows = $this->rows;
        \usort($rows, fn (Row $a, Row $b) : int => -($a->valueOf($ref) <=> $b->valueOf($ref)));

        return new self(...$rows);
    }

    public function sortEntries() : self
    {
        return $this->map(fn (Row $row) : Row => $row->sortEntries());
    }

    public function take(int $size) : self
    {
        return new self(...\array_slice($this->rows, 0, $size));
    }

    public function takeRight(int $size) : self
    {
        return new self(...\array_reverse(\array_slice($this->rows, -$size, $size)));
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function toArray() : array
    {
        $array = [];

        foreach ($this->rows as $row) {
            $array[] = $row->toArray();
        }

        return $array;
    }

    public function unique(Comparator $comparator = new NativeComparator()) : self
    {
        /**
         * @var Row[] $uniqueRows
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

        return new self(...$uniqueRows);
    }
}
