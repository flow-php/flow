<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\cache;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class CacheTest extends FlowTestCase
{
    public function test_cache_through_invoke(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame(
            [['a' => 1], ['a' => 2]],
            cache('pipe', cache: $storage)(df()->read(from_array([['a' => 1], ['a' => 2]])))->fetch()->toArray(),
        );
        static::assertTrue($storage->has('pipe'));
    }

    public function test_cache_through_with(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame(
            [['a' => 1], ['a' => 2]],
            df()
                ->read(from_array([['a' => 1], ['a' => 2]]))
                ->with(cache('pipe', cache: $storage))
                ->fetch()
                ->toArray(),
        );
        static::assertTrue($storage->has('pipe'));
    }
}
