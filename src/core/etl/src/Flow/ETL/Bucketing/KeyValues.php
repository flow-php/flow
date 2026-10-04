<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

/**
 * Extracts bucket-key values positionally, in reference order - position, not column name, defines
 * key identity, so two sides of a join extract hash-compatible values from differently named columns.
 * Each value is its column's equality form (ComparableValues): === on it is value equality.
 */
final readonly class KeyValues
{
    /**
     * @param list<Reference> $refs
     */
    public function __construct(
        private array $refs,
    ) {}

    /**
     * @return list<list<mixed>>
     */
    public function of(Rows $rows): array
    {
        if ($rows->isEmpty()) {
            return [];
        }

        $values = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $values[] = [];
        }

        $comparable = new ComparableValues();

        foreach ($this->refs as $ref) {
            $column = $rows->column($rows->schema()->get($ref)->entry()->name());

            // @mago-ignore analysis:mixed-assignment
            foreach ($comparable->equality($column) as $i => $value) {
                $values[$i][] = $value;
            }
        }

        return $values;
    }
}
