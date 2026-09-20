<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\WindowColumn;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;

final class WindowColumnTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new WindowColumn($input, 'rank', rank()->over(window()->orderBy(ref('id'))));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $function = rank()->over(window()->orderBy(ref('id')));
        $node = new WindowColumn($input, 'rank', $function);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(WindowColumn::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame('rank', $rebuilt->entry);
        static::assertSame($function, $rebuilt->function);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new WindowColumn($input, 'rank', rank()->over(window()->orderBy(ref('id'))));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::blocking, $node->materialization());
        static::assertEquals(Redefined::names('rank'), $node->redefines());
    }
}
