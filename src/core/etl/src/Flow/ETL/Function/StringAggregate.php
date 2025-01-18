<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\{Reference, SortOrder};

final class StringAggregate implements AggregatingFunction
{
    /**
     * @var array<string>
     */
    private array $values = [];

    public function __construct(private readonly Reference $ref, private readonly string $separator, private readonly ?SortOrder $sort = null)
    {
    }

    public function aggregate(Row $row) : void
    {
        $stringValue = (new Parameter($this->ref))->asString($row);

        if ($stringValue !== null) {
            $this->values[] = $stringValue;
        }
    }

    public function result() : StringEntry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_str_agg');
        }

        if (!\count($this->values)) {
            return str_entry($this->ref->name(), '');
        }

        if ($this->sort) {
            $this->sort === SortOrder::ASC ?
                \sort($this->values)
                : \rsort($this->values);
        }

        return str_entry($this->ref->name(), \implode($this->separator, $this->values));
    }
}
