<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\refs;

final class AggregateTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Aggregate($input, new GroupBy(refs('id')), hash_group_by());

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $groupBy = new GroupBy(refs('id'));
        $algorithm = hash_group_by();
        $node = new Aggregate($input, $groupBy, $algorithm);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Aggregate::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($groupBy, $rebuilt->groupBy);
        static::assertSame($algorithm, $rebuilt->algorithm);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Aggregate($input, new GroupBy(refs('id')), hash_group_by());

        static::assertSame(RowCount::reducing, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }
}
