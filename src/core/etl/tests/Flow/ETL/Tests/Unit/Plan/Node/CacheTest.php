<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Cache;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class CacheTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Cache($input, 'cache-id', 100, new InMemoryCache());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $cache = new InMemoryCache();
        $node = new Cache($input, 'cache-id', 100, $cache);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Cache::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame('cache-id', $rebuilt->id);
        static::assertSame(100, $rebuilt->batchSize);
        static::assertSame($cache, $rebuilt->cache);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Cache($input, 'cache-id', 100, new InMemoryCache());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
