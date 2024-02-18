<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class EntryIdFactory implements IdFactory
{
    public function __construct(private string $entryName)
    {
    }

    public function create(Row $row) : Entry
    {
        return $row->get($this->entryName)->rename('id');
    }
}
