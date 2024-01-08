<?php declare(strict_types=1);

namespace Flow\ETL\Row;

interface EntryFactory
{
    public function create(string $entryName, mixed $value, ?Schema $schema = null) : Entry;
}
