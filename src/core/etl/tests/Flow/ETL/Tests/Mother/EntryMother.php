<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Plan\Explain\Entry;
use stdClass;

final class EntryMother
{
    /**
     * @param list<Entry> $children
     * @param list<string> $lines
     */
    public static function named(
        string $name,
        ?int $number = 1,
        array $children = [],
        array $lines = [],
        string $suffix = '',
    ): Entry {
        return new Entry(new stdClass(), $name, $lines, $number, false, $children, $suffix);
    }
}
