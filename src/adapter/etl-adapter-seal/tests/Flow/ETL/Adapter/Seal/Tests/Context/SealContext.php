<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Context;

use CmsIg\Seal\Adapter\Memory\MemoryAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;

use function array_keys;

final class SealContext
{
    /**
     * @var list<array{EngineInterface, string}>
     */
    private array $indexes = [];

    public function dropIndexes(): void
    {
        foreach ($this->indexes as [$engine, $index]) {
            if ($engine->existIndex($index)) {
                $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
            }
        }

        $this->indexes = [];
    }

    public function engine(Schema $schema): EngineInterface
    {
        $engine = new Engine(new MemoryAdapter(), $schema);

        foreach (array_keys($schema->indexes) as $index) {
            if ($engine->existIndex($index)) {
                $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
            }

            $engine->createIndex($index, ['return_slow_promise_result' => true])?->wait();

            $this->indexes[] = [$engine, $index];
        }

        return $engine;
    }
}
