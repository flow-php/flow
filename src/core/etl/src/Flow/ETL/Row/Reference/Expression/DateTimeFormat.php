<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class DateTimeFormat implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $format
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $entry = $row->get($this->ref);

        if (!$entry instanceof Row\Entry\DateTimeEntry) {
            throw new \InvalidArgumentException("Entry {$this->ref} is not a DateTimeEntry");
        }

        return $entry->value()->format($this->format);
    }
}
