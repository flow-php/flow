<?php

use Rector\Config\RectorConfig;
use Rector\DeadCode\Rector\StaticCall\RemoveParentCallWithoutParentRector;
use Rector\Set\ValueObject\LevelSetList;

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
    ->withSkip([
        RemoveParentCallWithoutParentRector::class
    ]);