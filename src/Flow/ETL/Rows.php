<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Comparator;
use Flow\ETL\Row\Comparator\NativeComparator;
use Flow\Serializer\Serializable;

/**
 * @implements \ArrayAccess<int, Row>
 * @implements \IteratorAggregate<int, Row>
 * @psalm-immutable
 */
final class Rows implements \ArrayAccess, \Countable, \IteratorAggregate, Serializable
{
    /**
     * @var array<int, Row>
     */
    private array $rows;

    private bool $first;

    private bool $last;

    public function __construct(Row ...$rows)
    {
        $this->rows = \array_values($rows);
        $this->first = true;
        $this->last = false;
    }

    /**
     * @return array{rows: array<int, Row>, first: boolean, last: boolean}
     */
    public function __serialize() : array
    {
        return [
            'rows' => $this->rows,
            'first' => $this->first,
            'last' => $this->last,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{rows: array<int, Row>, first: boolean, last: boolean} $data
     */
    public function __unserialize(array $data) : void
    {
        $this->rows = $data['rows'];
        $this->first = $data['first'];
        $this->last = $data['last'];
    }

    public function makeFirst() : self
    {
        $rows = new self(...$this->rows);
        $rows->first = true;
        $rows->last = $this->last;

        return $rows;
    }

    public function makeLast() : self
    {
        $rows = new self(...$this->rows);
        $rows->last = true;
        $rows->first = $this->first;

        return $rows;
    }

    public function isFirst() : bool
    {
        return $this->first;
    }

    public function isLast() : bool
    {
        return $this->last;
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
     * @return \Iterator<int, Row>
     */
    public function getIterator() : \Iterator
    {
        return new \ArrayIterator($this->rows);
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

    public function sortAscending(string $name) : self
    {
        $rows = $this->rows;
        \usort($rows, fn (Row $a, Row $b) : int => $a->valueOf($name) <=> $b->valueOf($name));

        return new self(...$rows);
    }

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

    public function first() : Row
    {
        if (empty($this->rows)) {
            throw RuntimeException::because('First row does not exist in empty collection');
        }

        return \reset($this->rows);
    }

    public function empty() : bool
    {
        return $this->count() === 0;
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
    public function find(callable $callable) : ?Row
    {
        $rows = $this->rows;

        if (!\count($rows)) {
            return null;
        }

        $results = [];

        foreach ($this->rows as $row) {
            if ($callable($row)) {
                $results[] = $row;
            }
        }

        if (\count($results)) {
            return \current($results);
        }

        return null;
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

    public function count() : int
    {
        return \count($this->rows);
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

    public function add(Row ...$rows) : self
    {
        return new self(
            ...\array_merge($this->rows, $rows)
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

    public function drop(int $size) : self
    {
        $rows = $this->rows;

        for ($i = 0; $i < $size; $i++) {
            \array_shift($rows);
        }

        $newRows = new self(...$rows);
        $newRows->first = $this->first;
        $newRows->last = $this->last;

        return $newRows;
    }

    public function take(int $size) : self
    {
        $rows = $this->rows;
        $newRows = [];

        for ($i = 0; $i < $size; $i++) {
            $newRows[] = \array_shift($rows);
        }

        $newRows = new self(...\array_filter($newRows));
        $newRows->first = $this->first;
        $newRows->last = $this->last;

        return $newRows;
    }

    public function dropRight(int $size) : self
    {
        $rows = $this->rows;

        for ($i = 0; $i < $size; $i++) {
            \array_pop($rows);
        }

        $newRows = new self(...$rows);
        $newRows->first = $this->first;
        $newRows->last = $this->last;

        return $newRows;
    }

    public function takeRight(int $size) : self
    {
        $rows = $this->rows;
        $newRows = [];

        for ($i = 0; $i < $size; $i++) {
            $newRows[] = \array_pop($rows);
        }

        $newRows = new self(...\array_filter($newRows));
        $newRows->first = $this->first;
        $newRows->last = $this->last;

        return $newRows;
    }

    public function reverse() : self
    {
        $newRows = new self(...\array_reverse($this->rows));
        $newRows->first = $this->first;
        $newRows->last = $this->last;

        return $newRows;
    }
}
