<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

/**
 * Extracts bucket-key values positionally, in reference order - position, not column name, defines
 * key identity, so two sides of a join extract hash-compatible values from differently named columns.
 */
final readonly class KeyValues
{
    /**
     * @param list<Reference> $refs
     */
    public function __construct(
        private array $refs,
        private bool $nullOnMissing = false,
    ) {}

    /**
     * @return list<list<mixed>>
     */
    public function of(Rows $rows): array
    {
        $values = [];

        foreach ($rows as $row) {
            $values[] = $this->ofRow($row);
        }

        return $values;
    }

    /**
     * @return list<mixed>
     */
    public function ofRow(Row $row): array
    {
        $values = [];

        foreach ($this->refs as $ref) {
            $values[] = $this->nullOnMissing && !$row->has($ref) ? null : $row->get($ref);
        }

        return $values;
    }
}
