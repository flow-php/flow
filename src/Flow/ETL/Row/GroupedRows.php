<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class GroupedRows
{
    /**
     * @var array<string>
     */
    private array $entries;

    /**
     * @var array<RowsGroup>
     */
    private array $groups;

    public function __construct(string ...$entries)
    {
        if (!\count($entries)) {
            throw new InvalidArgumentException('Group by requires at least one entry name, none given.');
        }

        $this->groups = [];
        $this->entries = $entries;
    }

    public function add(Row $row) : void
    {
        $groupedRow = $row;

        foreach ($this->entries as $entry) {
            if (!$groupedRow->entries()->has($entry)) {
                $groupedRow = $groupedRow->add(\Flow\ETL\DSL\Entry::null($entry));
            }
        }

        $values = $groupedRow->entries()->getAll(...$this->entries)->map(fn (Entry $entry) => $entry->toString());
        $hash = \hash('sha256', \implode($values));

        if (!\array_key_exists($hash, $this->groups)) {
            $this->groups[$hash] = new RowsGroup($this->entries, $values);
            $this->groups[$hash]->add($groupedRow);
        } else {
            $this->groups[$hash]->add($groupedRow);
        }
    }

    public function toRows() : Rows
    {
        $rows = new Rows();

        foreach ($this->groups as $group) {
            $rows = $rows->add(
                Row::create(
                    new Row\Entry\ArrayEntry('entries', $group->entries()),
                    new Row\Entry\ArrayEntry('values', $group->values()),
                    new Row\Entry\CollectionEntry(
                        'rows',
                        ...$group->groupEntries()
                    )
                )
            );
        }

        return $rows;
    }
}
