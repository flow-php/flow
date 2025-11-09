<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\to_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Row\EntryFactory;

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

    public function aggregate(Row $row, FlowContext $context) : void
    {
        try {
            /** @var array<string, mixed> $values */
            $values = [];

            $values[$this->ref->name()] = $row->valueOf($this->ref);

            $this->collection[] = \current($values);
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Collect error: ' . $e->getMessage()));
        }
    }

    /**
     * @return Entry<mixed>
     */
    public function result(EntryFactory $entryFactory) : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->name() . '_collection');
        }

        return to_entry($this->ref->name(), $this->collection, $entryFactory);
    }
}
