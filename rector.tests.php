<?php

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Tools\Rector\NewObjectToFunction;
use Flow\Tools\Rector\NewToFunctionCallRector;
use Rector\Config\RectorConfig;
use Rector\DeadCode\Rector\StaticCall\RemoveParentCallWithoutParentRector;
use Rector\Set\ValueObject\LevelSetList;
use Rector\Transform\Rector\StaticCall\StaticCallToFuncCallRector;
use \Rector\Transform\ValueObject\StaticCallToFuncCall;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/src/core/etl/tests',
        __DIR__ . '/src/cli/tests',
        __DIR__ . '/src/adapter/*/tests',
        __DIR__ . '/src/bridge/*/*/tests',
        __DIR__ . '/src/tools/*/*/tests',
    ])
    ->withSets([
        LevelSetList::UP_TO_PHP_82
    ])
    ->withConfiguredRule(
        StaticCallToFuncCallRector::class,
        [
            new StaticCallToFuncCall(Flow\ETL\Row::class, 'create', 'Flow\ETL\DSL\row'),
            new StaticCallToFuncCall(Config::class, 'default', 'Flow\ETL\DSL\config')
        ]
    )
    ->withConfiguredRule(
        NewToFunctionCallRector::class,
        [
            new NewObjectToFunction(Rows::class, 'Flow\ETL\DSL\rows'),
            new NewObjectToFunction(Config::class, 'Flow\ETL\DSL\config'),
            new NewObjectToFunction(FlowContext::class, 'Flow\ETL\DSL\flow_context'),
        ]
    )
    ->withSkip([
        RemoveParentCallWithoutParentRector::class
    ]);