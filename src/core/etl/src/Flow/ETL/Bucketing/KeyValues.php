<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;

final readonly class KeyValues
{
    public function __construct(
        private References $refs,
    ) {}

    /**
     * @return list<array<string, mixed>>
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
     * @return array<string, mixed>
     */
    public function ofRow(Row $row): array
    {
        $values = [];

        foreach ($this->refs as $ref) {
            $values[$ref->name()] = $row->valueOf($ref);
        }

        return $values;
    }
}
