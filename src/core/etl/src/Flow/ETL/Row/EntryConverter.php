<?php declare(strict_types=1);

namespace Flow\ETL\Row;

interface EntryConverter
{
    public function convert(Entry $entry) : Entry;
}
