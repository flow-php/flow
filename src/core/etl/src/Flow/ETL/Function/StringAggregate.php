<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;

use function count;
use function Flow\ETL\DSL\str_entry;
use function implode;
use function is_string;
use function rsort;
use function sort;

final class StringAggregate implements AggregatingFunction
{
    /**
     * @var array<string>
     */
    private array $values = [];

    public function __construct(
        private readonly Reference $ref,
        private readonly string $separator,
        private readonly ?SortOrder $sort = null,
    ) {}

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref->to())) {
            return;
        }

        $stringValue = $row->valueOf($this->ref->to());

        if (is_string($stringValue)) {
            $this->values[] = $stringValue;
        }
    }

    /**
     * @return Row\Entry<?string>
     */
    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->to() . '_str_agg');

        if (!count($this->values)) {
            return str_entry($ref->name(), '');
        }

        if ($this->sort) {
            $this->sort === SortOrder::ASC ? sort($this->values) : rsort($this->values);
        }

        return str_entry($ref->name(), implode($this->separator, $this->values));
    }
}
