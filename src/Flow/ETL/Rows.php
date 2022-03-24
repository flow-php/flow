<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Sort;
use Flow\Serializer\Serializable;

/**
 * @implements \ArrayAccess<int, Row>
 * @implements \IteratorAggregate<int, Row>
 * @implements Serializable<array{rows: array<int, Row>}>
 * @psalm-immutable
 */
final class Rows implements \ArrayAccess, \Countable, \IteratorAggregate, Serializable
{
    /**
     * @var array<int, Row>
     */
    private array $rows;

    public function __construct(Row ...$rows)
    {
        $this->rows = \array_values($rows);
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
            ...\array_merge($this->rows, $rows)
        );
    }

    /**
     * @return Rows[]
     */
    public function chunks(int $size) : array
    {
        if ($size < 1) {
            throw InvalidArgumentException::because('Chunk size must be greater than 0');
        }

        $chunks = [];

        foreach (\array_chunk($this->rows, $size) as $chunk) {
            $chunks[] = new self(...$chunk);
        }

        return $chunks;
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
        $rows = $this->rows;

        for ($i = 0; $i < $size; $i++) {
            \array_shift($rows);
        }

        return new self(...$rows);
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
     * @psalm-suppress UnusedFunctionCall
     * @psalm-param pure-callable(Row) : void $callable
     *
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
     * @psalm-param pure-callable(Row) : bool $callable
     *
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

    /**
     * @psalm-param pure-callable(Row) : bool $callable
     */
    public function find(callable $callable) : self
    {
        $rows = $this->rows;

        if (!\count($rows)) {
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

    /**
     * @psalm-param pure-callable(Row) : bool $callable
     */
    public function findOne(callable $callable) : ?Row
    {
        $rows = $this->rows;

        if (!\count($rows)) {
            return null;
        }

        $rows = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $rows[] = $row;
            }
        }

        if (\count($rows)) {
            return \current($rows);
        }

        return null;
    }

    public function first() : Row
    {
        if (empty($this->rows)) {
            throw new RuntimeException('First row does not exist in empty collection');
        }

        return \reset($this->rows);
    }

    /**
     * @psalm-param pure-callable(Row) : Row[] $callable
     *
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

    /**
     * @psalm-param pure-callable(Row) : Row $callable
     *
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
     *
     * @return bool
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
     *
     * @return Row
     */
    public function offsetGet($offset) : Row
    {
        if ($this->offsetExists($offset)) {
            return $this->rows[$offset];
        }

        throw new InvalidArgumentException("Row {$offset} does not exists.");
    }

    public function offsetSet($offset, $value) : self
    {
        throw new RuntimeException('In order to add new rows use Rows::add(Row $row) : self');
    }

    /**
     * @param int $offset
     *
     * @throws InvalidArgumentException
     *
     * @return Rows
     * @psalm-suppress ImplementedReturnTypeMismatch
     */
    public function offsetUnset($offset) : self
    {
        throw new RuntimeException('In order to add new rows use Rows::remove(int $offset) : self');
    }

    /**
     * @psalm-param pure-callable(mixed, Row) : mixed $callable
     *
     * @param callable(mixed, Row) : mixed $callable
     * @param null|mixed $input
     *
     * @return null|mixed
     */
    public function reduce(callable $callable, $input = null)
    {
        return \array_reduce($this->rows, $callable, $input);
    }

    /**
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress NullableReturnStatement
     * @psalm-suppress MixedInferredReturnType
     *
     * @param string $entryName
     *
     * @return mixed[]
     */
    public function reduceToArray(string $entryName) : array
    {
        /** @phpstan-ignore-next-line */
        return $this->reduce(
            /** @phpstan-ignore-next-line */
            function (array $ids, Row $row) use ($entryName) : array {
                $ids[] = $row->get($entryName)->value();

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

        /** @psalm-suppress ImpureFunctionCall */
        $rows = \iterator_to_array($this->getIterator());
        unset($rows[$offset]);

        return new self(...\array_merge($rows));
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
     * @psalm-param pure-callable(Row, Row) : int $callback
     *
     * @return $this
     */
    public function sort(callable $callback) : self
    {
        $rows = $this->rows;
        \usort($rows, $callback);

        return new self(...$rows);
    }

    /**
     * @param string $name
     *
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortAscending(string $name) : self
    {
        $rows = $this->rows;
        \usort($rows, fn (Row $a, Row $b) : int => $a->valueOf($name) <=> $b->valueOf($name));

        return new self(...$rows);
    }

    /**
     * @param Sort ...$entries
     *
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortBy(Sort ...$entries) : self
    {
        $sortBy = \array_reverse($entries);

        $rows = $this;

        foreach ($sortBy as $entry) {
            $rows = $entry->isAsc() ? $rows->sortAscending($entry->name()) : $rows->sortDescending($entry->name());
        }

        return $rows;
    }

    /**
     * @param string $name
     *
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function sortDescending(string $name) : self
    {
        $rows = $this->rows;
        \usort($rows, fn (Row $a, Row $b) : int => -($a->valueOf($name) <=> $b->valueOf($name)));

        return new self(...$rows);
    }

    public function sortEntries() : self
    {
        return $this->map(fn (Row $row) : Row => $row->sortEntries());
    }

    public function take(int $size) : self
    {
        $rows = $this->rows;
        $newRows = [];

        for ($i = 0; $i < $size; $i++) {
            $newRows[] = \array_shift($rows);
        }

        return new self(...\array_filter($newRows));
    }

    public function takeRight(int $size) : self
    {
        $rows = $this->rows;
        $newRows = [];

        for ($i = 0; $i < $size; $i++) {
            $newRows[] = \array_pop($rows);
        }

        return new self(...\array_filter($newRows));
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

    public function unique(Comparator $comparator = null) : self
    {
        $comparator = $comparator === null ? new NativeComparator() : $comparator;

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
