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

function to_csv(string $file_name) : Loader
{
    if (!\class_exists('League\Csv\Reader')) {
        throw new RuntimeException("League\Csv\Reader class not found, please install it using 'composer require league/csv'");
    }

    return new LeagueCSVLoader(Writer::createFromPath($file_name, 'w+'));
}

/**
 * @param Client $client
 * @param int $chunk_size
 * @param string $index
 * @param IdFactory $id_factory
 * @param array<mixed> $parameters
 */
function to_elastic_search(Client $client, int $chunk_size, string $index, IdFactory $id_factory, array $parameters = []) : Loader
{
    return new ElasticsearchPHPLoader($client, $chunk_size, $index, $id_factory, $parameters);
}

function es_id_sha1(string ...$columns) : IdFactory
{
    return new Sha1IdFactory(...$columns);
}

function es_id_columns(string $column) : IdFactory
{
    return new EntryIdFactory($column);
}

function to_memory(Memory $memory) : Loader
{
    return new Loader\MemoryLoader($memory);
}

function to_debug_logger() : Loader
{
    return new PsrLoggerLoader(new DumpLogger(), 'debug row content');
}

function to_column_dumper(bool $all = false) : Loader
{
    return new class($all) implements Loader {
        private bool $allRows;

        private bool $dumped;

        public function __construct(bool $allRows)
        {
            $this->allRows = $allRows;
            $this->dumped = false;
        }

        /**
         * @psalm-suppress UnusedMethodCall
         * @psalm-suppress ForbiddenCode
         * @psalm-suppress InvalidArgument
         */
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
