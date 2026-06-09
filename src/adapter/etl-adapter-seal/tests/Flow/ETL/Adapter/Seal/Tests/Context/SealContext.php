<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Context;

use CmsIg\Seal\Adapter\Memory\MemoryAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;

use function Flow\ETL\Adapter\Seal\seal_create_index;
use function Flow\ETL\Adapter\Seal\seal_drop_index;

final class SealContext
{
    private readonly EngineInterface $engine;

    public function __construct(
        Schema $schema,
        private readonly string $indexName,
    ) {
        $this->engine = new Engine(new MemoryAdapter(), $schema);

        if ($this->engine->existIndex($this->indexName)) {
            seal_drop_index($this->engine, $this->indexName);
        }

        seal_create_index($this->engine, $this->indexName);
    }

    public function dropIndex(): void
    {
        if ($this->engine->existIndex($this->indexName)) {
            seal_drop_index($this->engine, $this->indexName);
        }
    }

    public function engine(): EngineInterface
    {
        return $this->engine;
    }

    public function indexName(): string
    {
        return $this->indexName;
    }

    public function refresh(): void {}
}
