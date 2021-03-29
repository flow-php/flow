<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Loader;
use Flow\ETL\Rows;
use League\Csv\Writer;

/**
 * @psalm-immutable
 */
final class LeagueCSVLoader implements Loader
{
    private Writer $writer;

    private string $rowEntryName;

    public function __construct(Writer $writer, string $rowEntryName = 'row')
    {
        $this->writer = $writer;
        $this->rowEntryName = $rowEntryName;
    }

    public function load(Rows $rows) : void
    {
        /** @psalm-suppress ImpureMethodCall */
        $this->writer->insertAll($rows->reduceToArray($this->rowEntryName));
    }
}
