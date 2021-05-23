<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Extractor;

use Flow\ETL\Adapter\CSV\LeagueCSVExtractor;
use Flow\ETL\Adapter\JSON\JSONMachineExtractor;
use Flow\ETL\ETL;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use JsonMachine\JsonMachine;
use League\Csv\Reader;

function extractCSV(string $fileName, int $batchSize = 100, int $headerOffset = 0) : ETL
{
    if (!\class_exists('League\Csv\Reader')) {
        throw new RuntimeException("League\Csv\Reader class not found, please require using 'composer require league/csv'");
    }

    $reader = Reader::createFromPath($fileName, 'r');
    $reader->setHeaderOffset($headerOffset);

    return ETL::extract(new LeagueCSVExtractor($reader, $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}

function extractArray(array $array, int $batchSize = 100) : ETL
{
    return ETL::extract(new MemoryExtractor(new ArrayMemory($array), $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}

function extractJSON(string $fileName, int $batchSize = 100) : ETL
{
    if (!\class_exists('JsonMachine\JsonMachine')) {
        throw new RuntimeException("JsonMachine\JsonMachine class not found, please require using 'composer require halaxa/json-machine'");
    }

    return ETL::extract(new JSONMachineExtractor(JsonMachine::fromFile($fileName), $batchSize, $entryRowName = 'row'))
        ->transform(new ArrayUnpackTransformer($entryRowName));
}
