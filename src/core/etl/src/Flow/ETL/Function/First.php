<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\{Entry, Reference};

final class First implements AggregatingFunction
{
    /**
     * @var null|Entry<mixed, mixed>
     */
    private ?Entry $first;

    public function __construct(private readonly Reference $ref)
    {
        $this->first = null;
    }

    public function aggregate(Row $row) : void
    {
        if ($this->first === null) {
            try {
                $this->first = $row->get($this->ref);
            } catch (InvalidArgumentException) {
                // entry not found
            }
        }
    }

    /**
     * @return Entry<mixed, mixed>
     */
    public function result() : Entry
    {
        $name = $this->ref->hasAlias() ? $this->ref->name() : $this->ref->name() . '_first';

        if ($this->first) {
            return $this->first->rename($name);
        }

        return string_entry($name, null);
    }
}
