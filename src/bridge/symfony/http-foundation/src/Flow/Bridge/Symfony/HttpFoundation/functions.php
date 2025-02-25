<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\ETL\Extractor;

function http_stream_open(Extractor $extractor) : DataStream
{
    return DataStream::open($extractor);
}

function http_json_output() : Output\JsonOutput
{
    return new Output\JsonOutput();
}

function http_csv_output() : Output\CSVOutput
{
    return new Output\CSVOutput();
}

function http_xml_output() : Output\XMLOutput
{
    return new Output\XMLOutput();
}

function http_parquet_output() : Output\ParquetOutput
{
    return new Output\ParquetOutput();
}
