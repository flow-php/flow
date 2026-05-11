<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\FrameBound;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\WindowDefinition;
use Flow\PostgreSql\QueryBuilder\Clause\WindowFrame;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class WindowDefinitionTest extends TestCase
{
    public function test_named_window(): void
    {
        $window = new WindowDefinition('w');

        static::assertSame('w', $window->name());
        static::assertEmpty($window->partitionBy());
        static::assertEmpty($window->orderBy());
        static::assertNull($window->frame());
        static::assertNull($window->refName());
    }

    public function test_to_ast(): void
    {
        $window = new WindowDefinition('w');
        $node = $window->toAst();
        $windowDef = $node->getWindowDef();

        static::assertNotNull($windowDef);
        static::assertSame('w', $windowDef->getName());
    }

    public function test_window_with_frame(): void
    {
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $window = new WindowDefinition('w', frame: $frame);

        static::assertNotNull($window->frame());
        static::assertSame($frame, $window->frame());
    }

    public function test_window_with_order_by(): void
    {
        $window = new WindowDefinition('w', orderBy: [new OrderBy(Column::name('created_at'))]);

        static::assertCount(1, $window->orderBy());
    }

    public function test_window_with_partition_by(): void
    {
        $window = new WindowDefinition('w', partitionBy: [Column::name('category')]);

        static::assertCount(1, $window->partitionBy());
    }

    public function test_window_with_ref_name(): void
    {
        $window = new WindowDefinition('w2', refName: 'w1');

        static::assertSame('w2', $window->name());
        static::assertSame('w1', $window->refName());
    }

    public function test_with_frame(): void
    {
        $window = new WindowDefinition('w');
        $frame = WindowFrame::rows(FrameBound::currentRow());
        $windowWithFrame = $window->withFrame($frame);

        static::assertNull($window->frame());
        static::assertNotNull($windowWithFrame->frame());
    }

    public function test_with_order_by(): void
    {
        $window = new WindowDefinition('w');
        $orderBy = [new OrderBy(Column::name('id'))];
        $windowWithOrder = $window->withOrderBy($orderBy);

        static::assertEmpty($window->orderBy());
        static::assertCount(1, $windowWithOrder->orderBy());
    }

    public function test_with_partition_by(): void
    {
        $window = new WindowDefinition('w');
        $partitionBy = [Column::name('status')];
        $windowWithPartition = $window->withPartitionBy($partitionBy);

        static::assertEmpty($window->partitionBy());
        static::assertCount(1, $windowWithPartition->partitionBy());
    }
}
