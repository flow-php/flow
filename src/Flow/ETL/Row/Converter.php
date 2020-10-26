<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @psalm-immutable
 */
interface Converter
{
    public function convert(Entry $entry) : Entry;
}
