<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node\Cache;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class CacheTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Cache($input, 'cache-id', 100, new MemoryRowsStorage());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $cache = new MemoryRowsStorage();
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
        $node = new Cache(NodeMother::read(), 'cache-id', 100, new MemoryRowsStorage());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
