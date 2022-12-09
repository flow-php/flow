<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Collect implements Aggregator
{
    /**
     * @var array<mixed>
     */
    private array $collection;

    public function __construct(private readonly string $entry)
    {
        $this->collection = [];
    }

    public function aggregate(Row $row) : void
    {
        try {
            $this->collection[] = $row->valueOf($this->entry);
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        return \Flow\ETL\DSL\Entry::array($this->entry . '_collection', $this->collection);
    }
}
