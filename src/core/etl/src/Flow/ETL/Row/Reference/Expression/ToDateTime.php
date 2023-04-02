<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ToDateTime implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $format,
        private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC')
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $entry = $row->get($this->ref);

        return match (\get_class($entry)) {
            Row\Entry\StringEntry::class => \DateTimeImmutable::createFromFormat($this->format, $entry->value(), $this->timeZone),
            Row\Entry\DateTimeEntry::class => $entry->value()->setTimezone($this->timeZone),
            Row\Entry\IntegerEntry::class => \DateTimeImmutable::createFromFormat('U', (string) $entry->value(), $this->timeZone),
            default => throw new \InvalidArgumentException("Entry {$this->ref} is not a DateTimeEntry")
        };
    }
}
