<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Extractor;

use Doctrine\DBAL\Connection;
use Flow\ETL\Adapter\CSV\LeagueCSVExtractor;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\Adapter\JSON\JSONMachineExtractor;
use Flow\ETL\ETL;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use JsonMachine\JsonMachine;
use League\Csv\Reader;
use Psr\Http\Client\ClientInterface;

function extract_from_csv(string $file_name, int $batch_size = 100, int $header_offset = 0) : ETL
{
    if (!\class_exists('League\Csv\Reader')) {
        throw new RuntimeException("League\Csv\Reader class not found, please install it using 'composer require league/csv'");
    }

    $reader = Reader::createFromPath($file_name, 'r');
    $reader->setHeaderOffset($header_offset);

    return ETL::extract(new LeagueCSVExtractor($reader, $batch_size, $entry_row_name = 'row'))
        ->transform(new ArrayUnpackTransformer($entry_row_name))
        ->transform(new RemoveEntriesTransformer($entry_row_name));
}

function extract_from_array(array $array, int $batch_size = 100) : ETL
{
    return ETL::extract(new MemoryExtractor(new ArrayMemory($array), $batch_size, $entry_row_name = 'row'))
        ->transform(new ArrayUnpackTransformer($entry_row_name))
        ->transform(new RemoveEntriesTransformer($entry_row_name));
}

function extract_from_json(string $file_name, int $batch_size = 100) : ETL
{
    if (!\class_exists('JsonMachine\JsonMachine')) {
        throw new RuntimeException("JsonMachine\JsonMachine class not found, please install it using 'composer require halaxa/json-machine'");
    }

    return ETL::extract(new JSONMachineExtractor(JsonMachine::fromFile($file_name), $batch_size, $entry_row_name = 'row'))
        ->transform(new ArrayUnpackTransformer($entry_row_name))
        ->transform(new RemoveEntriesTransformer($entry_row_name));
}

function extract_from_http(ClientInterface $client, iterable $requests, ?callable $pre_request = null, ?callable $post_request = null) : ETL
{
    if (!\class_exists('Psr\Http\Client\ClientInterface')) {
        throw new RuntimeException("Psr\Http\Client\ClientInterface class not found, please install one of available implementations https://packagist.org/providers/psr/http-client-implementation");
    }

    return ETL::extract(new PsrHttpClientStaticExtractor($client, $requests, $pre_request, $post_request));
}

function extract_from_http_dynamic(ClientInterface $client, NextRequestFactory $request_factory, ?callable $pre_request = null, ?callable $post_request = null) : ETL
{
    if (!\class_exists('Psr\Http\Client\ClientInterface')) {
        throw new RuntimeException("Psr\Http\Client\ClientInterface class not found, please install one of available implementations https://packagist.org/providers/psr/http-client-implementation");
    }

    return ETL::extract(new PsrHttpClientDynamicExtractor($client, $request_factory, $pre_request, $post_request));
}

function extract_from_db(Connection $connection, string $query, ParametersSet $parameters_set = null, array $types = []) : ETL
{
    return ETL::extract(new DbalQueryExtractor($connection, $query, $parameters_set, $types, $entry_row_name = 'row'))
        ->transform(new ArrayUnpackTransformer($entry_row_name))
        ->transform(new RemoveEntriesTransformer($entry_row_name));
}
