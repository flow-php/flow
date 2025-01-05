<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\{Entry, Reference};

final class Last implements AggregatingFunction
{
    /**
     * @var null|Entry<mixed, mixed>
     */
    private ?Entry $last;

    public function __construct(private readonly Reference $ref)
    {
        $this->last = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $this->last = $row->get($this->ref);
        } catch (InvalidArgumentException $e) {
            // entry not found
        }
    }

    /**
     * @return Entry<mixed, mixed>
     */
    public function result() : Entry
    {
        $name = $this->ref->hasAlias() ? $this->ref->name() : $this->ref->name() . '_last';

        if ($this->last) {
            return $this->last->rename($name);
        }

        return string_entry($name, null);
    }
}
