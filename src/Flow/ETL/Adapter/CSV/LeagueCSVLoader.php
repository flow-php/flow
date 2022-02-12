<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use League\Csv\Writer;

/**
 * @deprecated
 * @psalm-immutable
 */
final class LeagueCSVLoader implements Loader
{
    private Writer $writer;

    private bool $withHeader;

    private bool $headerAdded;

    public function __construct(Writer $writer, bool $withHeader = true)
    {
        $this->writer = $writer;
        $this->headerAdded = false;
        $this->withHeader = $withHeader;
    }

    public function __serialize() : array
    {
        throw new RuntimeException('DbalBulkLoader is not serializable, please use DbalLoader');
    }

    public function __unserialize(array $data) : void
    {
        throw new RuntimeException('DbalBulkLoader is not serializable, please use DbalLoader');
    }

    /**
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InaccessibleProperty
     */
    public function load(Rows $rows) : void
    {
        if ($this->withHeader && !$this->headerAdded) {
            $this->writer->insertOne($rows->first()->entries()->map(fn (Entry $entry) => $entry->name()));
            $this->headerAdded = true;
        }

        $this->writer->insertAll($rows->toArray());
    }
}
