<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Output\{CSVOutput, JsonOutput, ParquetOutput, XMLOutput};
use Flow\ETL\Dataset\Report;
use Flow\ETL\Extractor;

function http_stream_open(Extractor $extractor) : DataStream
{
    return DataStream::open($extractor);
}

function http_json_output() : JsonOutput
{
    return new JsonOutput();
}

function http_csv_output() : CSVOutput
{
    return new CSVOutput();
}

function http_xml_output() : XMLOutput
{
    return new XMLOutput();
}

function http_parquet_output() : ParquetOutput
{
    return new ParquetOutput();
}

/**
 * Create a StreamClosure from a callable.
 *
 * @param callable(?Report): void $callback
 */
function http_on_complete(callable $callback) : StreamClosure
{
    return new readonly class($callback) implements StreamClosure {
        public function __construct(
            /** @var callable(?Report): void */
            private mixed $callback,
        ) {
        }

        public function onComplete(?Report $report) : void
        {
            ($this->callback)($report);
        }
    };
}
