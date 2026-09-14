<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Roots;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\SpySink;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class RootsTest extends FlowTestCase
{
    public function test_a_loader_becomes_one_write_over_the_frames_root(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]))->select('id');
        $loader = to_memory(new ArrayMemory());

        $roots = (new Roots())->of($dataFrame, $loader);

        static::assertCount(1, $roots);
        static::assertInstanceOf(Write::class, $roots[0]);
        static::assertSame($loader, $roots[0]->loader);
        static::assertSame($dataFrame->cursor(), $roots[0]->children()[0]);
    }

    public function test_two_children_share_the_root_and_do_not_alias(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $root = $dataFrame->cursor();
        $roots = new Roots();

        $transformed = $roots->of(
            $dataFrame,
            new Transformed(new AddRowIndexTransformer('idx', StartFrom::ZERO), to_memory(new ArrayMemory())),
        )[0]->children()[0];
        $branched = $roots->of(
            $dataFrame,
            new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
        )[0]->children()[0];

        static::assertInstanceOf(Transform::class, $transformed);
        static::assertInstanceOf(Filter::class, $branched);
        static::assertSame([$root], $transformed->children());
        static::assertSame([$root], $branched->children());
        static::assertSame($root, $dataFrame->cursor());
    }

    public function test_a_transformation_writing_inside_a_sink_adds_its_own_root(): void
    {
        $inner = to_memory(new ArrayMemory());
        $outer = to_memory(new ArrayMemory());

        $roots = (new Roots())->of(df()->read(from_array([['id' => 1]])), new Branched(
            ref('id')->equals(lit(1)),
            new Transformed(new CallbackTransformation(
                static fn(DataFrame $prefix): DataFrame => $prefix->write($inner),
            ), $outer),
        ));

        static::assertCount(2, $roots);
        static::assertInstanceOf(Write::class, $roots[0]);
        static::assertInstanceOf(Write::class, $roots[1]);
        static::assertSame($outer, $roots[0]->loader);
        static::assertSame($inner, $roots[1]->loader);
    }

    public function test_the_sink_is_handed_a_fork_not_the_callers_frame(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $sink = new SpySink();

        static::assertSame([], (new Roots())->of($dataFrame, $sink));
        $prefix = $sink->prefix;
        static::assertInstanceOf(DataFrame::class, $prefix);
        static::assertSame($dataFrame->cursor(), $prefix->cursor());
        static::assertNotSame($dataFrame, $prefix);
    }
}
