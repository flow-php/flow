<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;

use function Flow\ETL\DSL\string_entry;

final class Last implements AggregatingFunction
{
    /**
     * @var null|Entry<mixed>
     */
    private ?Entry $last;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->last = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        $this->last = $row->get($this->ref);
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
        $name = $this->ref->hasAlias() ? $this->ref->name() : $this->ref->name() . '_last';

        if ($this->last) {
            return $this->last->rename($name);
        }

        return string_entry($name, null);
    }
}
