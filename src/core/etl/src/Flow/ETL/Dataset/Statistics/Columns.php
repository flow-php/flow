<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use Flow\ETL\Row\{Entry, EntryReference};

final class Columns
{
    /**
     * @var array<string, Column>
     */
    private array $columns = [];

    public function __construct()
    {
    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function add(Entry $entry) : void
    {
        if (!\array_key_exists($entry->name(), $this->columns)) {
            $this->columns[$entry->name()] = new Column($entry);

            return;
        }

        $this->columns[$entry->name()]->calculate($entry);
    }

    /**
     * @return array<Column>
     */
    public function all() : array
    {
        return \array_values($this->columns);
    }

    public function get(string|EntryReference $ref) : Column
    {
        if ($ref instanceof EntryReference) {
            $ref = $ref->name();
        }

        if (!\array_key_exists($ref, $this->columns)) {
            throw new \InvalidArgumentException(\sprintf('Column "%s" does not exist.', $ref));
        }

        return $this->columns[$ref];
    }
}
