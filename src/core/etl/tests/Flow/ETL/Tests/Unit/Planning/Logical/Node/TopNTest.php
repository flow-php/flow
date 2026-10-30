<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;

final class TopNTest extends FlowTestCase
{
    public function test_children_is_the_input(): void
    {
        $input = NodeMother::read();

        static::assertSame([$input], (new TopN($input, [ref('id')->asc()], 3))->children());
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new TopN($input, [ref('id')->asc()], 3);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_keeps_columns_and_limit_over_a_new_input(): void
    {
        $columns = [ref('id')->asc()];
        $replacement = NodeMother::read();

        $rebuilt = (new TopN(NodeMother::read(), $columns, 3))->withChildren([$replacement]);

        static::assertSame([$replacement], $rebuilt->children());
        static::assertSame($columns, $rebuilt->columns);
        static::assertSame(3, $rebuilt->limit);
    }

    public function test_a_limit_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TopN limit must be greater than 0, given: 0');

        new TopN(NodeMother::read(), [ref('id')->asc()], 0);
    }

    public function test_declarations(): void
    {
        $node = new TopN(NodeMother::read(), [ref('id')->asc()], 3);

        static::assertSame(RowCount::reducing, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
