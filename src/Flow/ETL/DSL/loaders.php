<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Loader;

use Elasticsearch\Client;
use Flow\ETL\Adapter\CSV\LeagueCSVLoader;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHPLoader;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\Sha1IdFactory;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Adapter\Logger\Logger\DumpLogger;
use Flow\ETL\Adapter\Logger\PsrLoggerLoader;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Writer;

function toCSV(string $fileName) : Loader
{
    if (!\class_exists('League\Csv\Reader')) {
        throw new RuntimeException("League\Csv\Reader class not found, please install it using 'composer require league/csv'");
    }

    return new LeagueCSVLoader(Writer::createFromPath($fileName, 'w+'));
}

function toElasticSearch(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = []) : Loader
{
    return new ElasticsearchPHPLoader($client, $chunkSize, $index, $idFactory, $parameters);
}

function esIdSha1(string ...$columns) : IdFactory
{
    return new Sha1IdFactory(...$columns);
}

function esIdColumns(string $column) : IdFactory
{
    return new EntryIdFactory($column);
}

function toMemory(Memory $memory) : Loader
{
    return new Loader\MemoryLoader($memory);
}

function toDebugLogger() : Loader
{
    return new PsrLoggerLoader(new DumpLogger(), 'debug row content');
}

function toColumnDumper(bool $all = false) : Loader
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
