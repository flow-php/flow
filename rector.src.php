<?php

use Rector\Php81\Rector\Array_\ArrayToFirstClassCallableRector;
use Rector\Config\RectorConfig;
use Rector\Php55\Rector\String_\StringClassNameToClassConstantRector;
use Rector\Set\ValueObject\LevelSetList;
use Rector\CodingStyle\Rector\FuncCall\FunctionFirstClassCallableRector;
use Rector\Php71\Rector\FuncCall\RemoveExtraParametersRector;
use Rector\CodingStyle\Rector\ArrowFunction\ArrowFunctionDelegatingCallToFirstClassCallableRector;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/bin',
        __DIR__ . '/examples/topics/*/*/*.php',
        __DIR__ . '/src/core/etl/src',
        __DIR__ . '/src/cli/src',
        __DIR__ . '/src/lib/*/src',
        __DIR__ . '/src/adapter/*/src',
        __DIR__ . '/src/bridge/*/*/src',
        __DIR__ . '/src/tools/*/src',
        __DIR__ . '/web/landing/src',
    ])
    ->withSkip([
        RemoveExtraParametersRector::class,
        FunctionFirstClassCallableRector::class,
        ArrowFunctionDelegatingCallToFirstClassCallableRector::class,
        StringClassNameToClassConstantRector::class,
        __DIR__ . '/src/lib/parquet/src/Flow/Parquet/ThriftModel/*',
        __DIR__ . '/src/bridge/telemetry/otlp/src/Opentelemetry/*',
        __DIR__ . '/src/bridge/telemetry/otlp/src/GPBMetadata/*',
        // rector 2.4.2 hangs parsing this file; revisit after upstream fix
        __DIR__ . '/src/core/etl/src/Flow/ETL/Formatter/ASCII/ASCIIValue.php',
        // Symfony DI requires array format for setFactory(), first-class callable syntax is not supported
        ArrayToFirstClassCallableRector::class => [
            __DIR__ . '/src/bridge/symfony/telemetry-bundle/src/Flow/Bridge/Symfony/TelemetryBundle/DependencyInjection/FlowTelemetryExtension.php',
            __DIR__ . '/src/bridge/symfony/filesystem-bundle/src/Flow/Bridge/Symfony/FilesystemBundle/DependencyInjection/Compiler/BuildFstabsPass.php',
            __DIR__ . '/src/bridge/symfony/postgresql-bundle/src/Flow/Bridge/Symfony/PostgreSqlBundle/DependencyInjection/FlowPostgreSqlExtension.php',
        ],
    ])
    ->withCache(__DIR__ . '/var/rector/src')
    ->withImportNames(importShortClasses: false, removeUnusedImports: true)
    ->withSets([
        LevelSetList::UP_TO_PHP_83,
    ]);
