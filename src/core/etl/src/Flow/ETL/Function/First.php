<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;

use function Flow\ETL\DSL\string_entry;

final class First implements AggregatingFunction
{
    /**
     * @var null|Entry<mixed>
     */
    private ?Entry $first;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->first = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        if ($this->first === null) {
            $this->first = $row->get($this->ref);
        }
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        $name = $this->ref->hasAlias() ? $this->ref->name() : $this->ref->name() . '_first';

        if ($this->first) {
            return $this->first->rename($name);
        }

        return string_entry($name, null);
    }
}
