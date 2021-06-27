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
use JsonMachine\JsonMachine;
use League\Csv\Reader;
use Psr\Http\Client\ClientInterface;

function extractFromCSV(string $fileName, int $batchSize = 100, int $headerOffset = 0) : ETL
{
    if (!\class_exists('League\Csv\Reader')) {
        throw new RuntimeException("League\Csv\Reader class not found, please install it using 'composer require league/csv'");
    }

    $reader = Reader::createFromPath($fileName, 'r');
    $reader->setHeaderOffset($headerOffset);

    return ETL::extract(new LeagueCSVExtractor($reader, $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}

function extractFromArray(array $array, int $batchSize = 100) : ETL
{
    return ETL::extract(new MemoryExtractor(new ArrayMemory($array), $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}

function extractFromJSON(string $fileName, int $batchSize = 100) : ETL
{
    if (!\class_exists('JsonMachine\JsonMachine')) {
        throw new RuntimeException("JsonMachine\JsonMachine class not found, please install it using 'composer require halaxa/json-machine'");
    }

    return ETL::extract(new JSONMachineExtractor(JsonMachine::fromFile($fileName), $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}

function extractFromHttp(ClientInterface $client, iterable $requests, ?callable $preRequest = null, ?callable $postRequest = null) : ETL
{
    if (!\class_exists('Psr\Http\Client\ClientInterface')) {
        throw new RuntimeException("Psr\Http\Client\ClientInterface class not found, please install one of available implementations https://packagist.org/providers/psr/http-client-implementation");
    }

    return ETL::extract(new PsrHttpClientStaticExtractor($client, $requests, $preRequest, $postRequest));
}

function extractFromHttpDynamic(ClientInterface $client, NextRequestFactory $requestFactory, ?callable $preRequest = null, ?callable $postRequest = null) : ETL
{
    if (!\class_exists('Psr\Http\Client\ClientInterface')) {
        throw new RuntimeException("Psr\Http\Client\ClientInterface class not found, please install one of available implementations https://packagist.org/providers/psr/http-client-implementation");
    }

    return ETL::extract(new PsrHttpClientDynamicExtractor($client, $requestFactory, $preRequest, $postRequest));
}

function extractFromDb(Connection $connection, string $query, ParametersSet $parametersSet = null, array $types = [])
{
    return ETL::extract(new DbalQueryExtractor($connection, $query, $parametersSet, $types, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}
