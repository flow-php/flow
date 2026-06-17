<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Context;

use Closure;
use CmsIg\Seal\Adapter\AdapterInterface;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;

final class SealContext
{
    /**
     * @var list<array{EngineInterface, string}>
     */
    private array $indexes = [];

    /**
     * @param null|Closure(): void $refresh
     */
    public function __construct(
        private readonly AdapterInterface $adapter,
        private readonly ?Closure $refresh = null,
    ) {}

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
        $engine = new Engine($this->adapter, $schema);

        foreach ($schema->indexes as $index => $_value) {
            if ($engine->existIndex($index)) {
                $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
            }

            $engine->createIndex($index, ['return_slow_promise_result' => true])?->wait();

            $this->indexes[] = [$engine, $index];
        }

        return $engine;
    }

    public function refresh(): void
    {
        if ($this->refresh !== null) {
            ($this->refresh)();
        }
    }
}
