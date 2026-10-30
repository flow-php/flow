<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Flow\Bridge\Symfony\HttpFoundation\Output\JsonOutput;
use Flow\Bridge\Symfony\HttpFoundation\Output\ParquetOutput;
use Flow\Bridge\Symfony\HttpFoundation\Output\XMLOutput;
use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Runtime\Metrics\Report;

function http_stream_open(Extractor|DataFrame $source): DataStream
{
    return DataStream::open($source);
}

function http_json_output(): JsonOutput
{
    return new JsonOutput();
}

function http_csv_output(): CSVOutput
{
    return new CSVOutput();
}

function http_xml_output(): XMLOutput
{
    return new XMLOutput();
}

function http_parquet_output(): ParquetOutput
{
    return new ParquetOutput();
}

/**
 * Create a StreamClosure from a callable.
 *
 * @param callable(?Report): void $callback
 */
function http_on_complete(callable $callback): StreamClosure
{
    return new readonly class($callback) implements StreamClosure {
        public function __construct(
            /** @var callable(?Report): void */
            private mixed $callback,
        ) {}

        public function onComplete(?Report $report): void
        {
            ($this->callback)($report);
        }
    };
}
