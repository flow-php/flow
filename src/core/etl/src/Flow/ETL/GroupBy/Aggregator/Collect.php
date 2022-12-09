<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;

final class Collect implements Aggregator
{
    /**
     * @var array<mixed>
     */
    private array $collection;

    private readonly Reference $entry;

    public function __construct(string|Reference $entry)
    {
        $this->entry = \is_string($entry) ? new EntryReference($entry) : $entry;
        $this->collection = [];
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var array<string, mixed> $values */
            $values = [];

            foreach ((array) $this->entry->to() as $entry) {
                /** @psalm-suppress MixedAssignment */
                $values[$entry] = $row->valueOf($entry);
            }

            if ($this->entry instanceof EntryReference) {
                $this->collection[] = \current($values);
            } else {
                $this->collection[] = $values;
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->entry->hasAlias()) {
            $this->entry->as($this->entry->name() . '_collection');
        }

        return \Flow\ETL\DSL\Entry::array($this->entry->name(), $this->collection);
    }
}
