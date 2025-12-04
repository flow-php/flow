<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Clause;

use Flow\PgQuery\QueryBuilder\Clause\{FrameBound, OrderBy, WindowDefinition, WindowFrame};
use Flow\PgQuery\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class WindowDefinitionTest extends TestCase
{
    public function test_named_window() : void
    {
        $window = new WindowDefinition('w');

        self::assertSame('w', $window->name());
        self::assertEmpty($window->partitionBy());
        self::assertEmpty($window->orderBy());
        self::assertNull($window->frame());
        self::assertNull($window->refName());
    }

    public function test_to_ast() : void
    {
        $window = new WindowDefinition('w');
        $node = $window->toAst();
        $windowDef = $node->getWindowDef();

        self::assertNotNull($windowDef);
        self::assertSame('w', $windowDef->getName());
    }

    public function test_window_with_frame() : void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $window = new WindowDefinition('w', frame: $frame);

        self::assertNotNull($window->frame());
        self::assertSame($frame, $window->frame());
    }

    public function test_window_with_order_by() : void
    {
        $window = new WindowDefinition(
            'w',
            orderBy: [new OrderBy(Column::name('created_at'))]
        );

        self::assertCount(1, $window->orderBy());
    }

    public function test_window_with_partition_by() : void
    {
        $window = new WindowDefinition(
            'w',
            partitionBy: [Column::name('category')]
        );

        self::assertCount(1, $window->partitionBy());
    }

    public function test_window_with_ref_name() : void
    {
        $window = new WindowDefinition('w2', refName: 'w1');

        self::assertSame('w2', $window->name());
        self::assertSame('w1', $window->refName());
    }

    public function test_with_frame() : void
    {
        $window = new WindowDefinition('w');
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $windowWithFrame = $window->withFrame($frame);

        self::assertNull($window->frame());
        self::assertNotNull($windowWithFrame->frame());
    }

    public function test_with_order_by() : void
    {
        $window = new WindowDefinition('w');
        $orderBy = [new OrderBy(Column::name('id'))];
        $windowWithOrder = $window->withOrderBy($orderBy);

        self::assertEmpty($window->orderBy());
        self::assertCount(1, $windowWithOrder->orderBy());
    }

    public function test_with_partition_by() : void
    {
        $window = new WindowDefinition('w');
        $partitionBy = [Column::name('status')];
        $windowWithPartition = $window->withPartitionBy($partitionBy);

        self::assertEmpty($window->partitionBy());
        self::assertCount(1, $windowWithPartition->partitionBy());
    }
}
