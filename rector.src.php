<?php

use Rector\Config\RectorConfig;
use Rector\Php55\Rector\String_\StringClassNameToClassConstantRector;
use Rector\Set\ValueObject\LevelSetList;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/bin',
        __DIR__ . '/examples/topics/*/*/*.php',
        __DIR__ . '/src/core/etl/src',
        __DIR__ . '/src/cli/src',
        __DIR__ . '/src/lib/*/src',
        __DIR__ . '/src/adapter/*/src',
        __DIR__ . '/src/bridge/*/*/src',
        __DIR__ . '/src/tools/*/*/src',
    ])
    ->withSkip([
        StringClassNameToClassConstantRector::class,
        __DIR__ . '/src/lib/parquet/src/Flow/Parquet/Thrift/*',
    ])
    ->withCache(__DIR__ . '/var/rector/src')
    ->withSets([
        LevelSetList::UP_TO_PHP_82
    ]);