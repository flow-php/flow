<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class EntryIdFactory implements IdFactory
{
    private string $entryName;

    public function __construct(string $entryName)
    {
        $this->entryName = $entryName;
    }

    public function create(Row $row) : Entry
    {
        return $row->get($this->entryName)->rename('id');
    }
}
