<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\CollectionEntry;

/**
 * @psalm-immutable
 */
final class Rows
{
    /**
     * @psalm-var array<int, Row>
     *
     * @var Row[]
     */
    private array $rows;

    public function __construct(Row ...$rows)
    {
        $this->rows = $rows;
    }

    /**
     * @psalm-suppress MixedArgument
     * @pslam-param callable (Row) : string|int|float $groupBy  Returns group identifier for the given Row
     */
    public function groupTo(string $collectionEntryName, callable $groupBy) : self
    {
        return new self(
            ...\array_reduce(
                $this->rows,
                function (Entries $entries, Row $row) use ($groupBy) : Entries {
                    $name = (string) $groupBy($row);

                    if (empty($name)) {
                        throw InvalidArgumentException::because('Group name for grouping rows cannot be empty');
                    }

                    if ($entries->has($name)) {
                        return $entries->appendTo($name, $row->entries());
                    }

                    return $entries->add(new CollectionEntry($name, $row->entries()));
                },
                new Entries()
            )->map(
                fn (Entry $entry) => Row::create($entry->rename($collectionEntryName))
            )
        );
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
        return new self(...\array_filter($this->rows, $callable));
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

        $results = \array_filter($rows, $callable);

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
        return new self(...\array_map($callable, $this->rows));
    }

    /**
     * @psalm-param pure-callable(Row) : void $callable
     *
     * @param callable(Row) : void $callable
     */
    public function each(callable $callable) : void
    {
        \array_map($callable, $this->rows);
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
        return $this->reduce(
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
        return \array_map(fn (Row $row) => $row->toArray(), $this->rows);
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

    public function add(Row $row) : self
    {
        return new self(
            ...\array_merge($this->rows, [$row])
        );
    }

    public function merge(self $rows) : self
    {
        return new self(
            ...\array_merge($this->rows, $rows->rows)
        );
    }
}
