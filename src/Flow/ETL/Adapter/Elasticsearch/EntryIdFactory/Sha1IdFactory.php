<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Sha1IdFactory implements IdFactory
{
    /**
     * @var string[]
     */
    private array $entryNames;

    public function __construct(string ...$entryNames)
    {
        $this->entryNames = $entryNames;
    }

    public function create(Row $row) : Entry
    {
        return new Row\Entry\StringEntry(
            'id',
            \sha1(
                \implode(':', \array_map(fn (string $name) => $row->valueOf($name), $this->entryNames))
            )
        );
    }
}
