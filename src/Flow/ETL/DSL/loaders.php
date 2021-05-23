<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Loader;

use Flow\ETL\Adapter\CSV\LeagueCSVLoader;
use Flow\ETL\Adapter\Logger\Logger\DumpLogger;
use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\Loader;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Writer;

function toCSV(string $fileName) : Loader
{
    return new LeagueCSVLoader(Writer::createFromPath($fileName, 'w+'));
}

function memory(Memory $memory) : Loader
{
    return new Loader\MemoryLoader($memory);
}

function debug() : Loader
{
    return new PsrLoggerLoader(new DumpLogger(), 'debug row content');
}

function debugEntries(bool $all = false) : Loader
{
    return new class($all) implements Loader {
        private bool $allRows;

        private bool $dumped;

        public function __construct(bool $allRows)
        {
            $this->allRows = $allRows;
            $this->dumped = false;
        }

        public function load(Rows $rows) : void
        {
            if ($this->allRows) {
                $rows->each(function (Row $row) : void {
                    foreach ($row->entries()->all() as $entry) {
                        \var_dump([$entry->name() => \get_class($entry)]);
                    }
                });
            } else {
                if (!$this->dumped) {
                    foreach ($rows->first()->entries()->all() as $entry) {
                        \var_dump([$entry->name() => \get_class($entry)]);
                    }

                    $this->dumped = true;
                }
            }
        }
    };
}
