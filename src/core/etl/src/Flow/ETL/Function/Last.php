<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Row\EntryFactory;

final class Last implements AggregatingFunction
{
    /**
     * @var null|Entry<mixed>
     */
    private ?Entry $last;

    public function __construct(private readonly Reference $ref)
    {
        $this->last = null;
    }

    public function aggregate(Row $row, FlowContext $context) : void
    {
        try {
            $this->last = $row->get($this->ref);
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Last error: ' . $e->getMessage()));
        }
    }

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory) : Entry
    {
        $name = $this->ref->hasAlias() ? $this->ref->name() : $this->ref->name() . '_last';

        if ($this->last) {
            return $this->last->rename($name);
        }

        return string_entry($name, null);
    }
}
