<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;

final class SortTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Sort($input, [ref('id')->asc()], memory_sort());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $columns = [ref('id')->asc()];
        $algorithm = memory_sort();
        $node = new Sort($input, $columns, $algorithm);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Sort::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($columns, $rebuilt->columns);
        static::assertSame($algorithm, $rebuilt->algorithm);
    }

    public function test_declarations(): void
    {
        $node = new Sort(NodeMother::read(), [ref('id')->asc()], memory_sort());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
