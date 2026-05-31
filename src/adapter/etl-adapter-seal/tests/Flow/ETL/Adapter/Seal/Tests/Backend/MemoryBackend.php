<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Backend;

use CmsIg\Seal\Adapter\Memory\MemoryAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;

trait MemoryBackend
{
    abstract protected function schema(): Schema;

    protected function createEngine(): EngineInterface
    {
        return new Engine(new MemoryAdapter(), $this->schema());
    }
}
