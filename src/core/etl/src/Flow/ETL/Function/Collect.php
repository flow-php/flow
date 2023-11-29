<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\array_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;

final class Collect implements AggregatingFunction
{
    /**
     * @var array<mixed>
     */
    private array $collection;

    public function __construct(private readonly Reference $ref)
    {
        $this->collection = [];
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var array<string, mixed> $values */
            $values = [];

            $values[$this->ref->name()] = $row->valueOf($this->ref);

            $this->collection[] = \current($values);
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->name() . '_collection');
        }

        return array_entry($this->ref->name(), $this->collection);
    }
}
