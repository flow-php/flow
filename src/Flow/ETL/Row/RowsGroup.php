<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class RowsGroup
{
    /**
     * @var array<string>
     */
    private array $entries;

    private Rows $group;

    /**
     * @var array<string>
     */
    private array $values;

    /**
     * @param array<string> $entries
     * @param array<string> $values
     * @param null|Rows $group
     */
    public function __construct(array $entries, array $values, Rows $group = null)
    {
        $this->entries = $entries;
        $this->values = $values;
        $this->group = $group ?? new Rows();
    }

    public function add(Row $row) : void
    {
        if (!$row->entries()->has(...$this->entries)) {
            throw new InvalidArgumentException('Given row is missing following entries: ' . \implode(', ', $this->entries));
        }

        $rowValues = $row->entries()->getAll(...$this->entries)->map(fn (Entry $entry) => $entry->toString());
        \sort($rowValues);
        $expectedValues = $this->values;
        \sort($expectedValues);

        if ($expectedValues !== $rowValues) {
            throw new InvalidArgumentException('Given row is missing following values: ' . \implode(', ', $this->values));
        }

        $this->group = $this->group->add($row);
    }

    /**
     * @return array<string>
     */
    public function entries() : array
    {
        return $this->entries;
    }

    /**
     * @return array<Entries>
     */
    public function groupEntries() : array
    {
        return $this->group->entries();
    }

    /**
     * @return array<string>
     */
    public function values() : array
    {
        return $this->values;
    }
}
