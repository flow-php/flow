<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\ProgressStages;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Planning\Physical\NodeTranslator;
use Flow\ETL\Runtime\Config;

final class NodeTranslatorMother
{
    /**
     * The translator $config builds into its planner.
     */
    public static function of(Config $config, ProgressStages $progress = new VoidProgress()): NodeTranslator
    {
        return new NodeTranslator(
            $config->backend(),
            $config->randomValueGenerator(),
            $config->id(),
            $config->cache->cache,
            $config->cache->localFilesystemCacheDir,
            $config->sort,
            $config->grouping,
            $config->join,
            $config->repartition,
            $progress,
        );
    }
}
