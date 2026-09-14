<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class SortTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Sort($input, refs(ref('id')), memory_sort());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $refs = refs(ref('id'));
        $algorithm = memory_sort();
        $node = new Sort($input, $refs, $algorithm);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Sort::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($refs, $rebuilt->refs);
        static::assertSame($algorithm, $rebuilt->algorithm);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Sort($input, refs(ref('id')), memory_sort());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::blocking, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
