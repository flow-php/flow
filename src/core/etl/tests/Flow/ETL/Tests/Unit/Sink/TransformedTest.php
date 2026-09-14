<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;

final class TransformedTest extends FlowTestCase
{
    public function test_a_transformer_becomes_a_write_over_a_transform(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $transformer = new AddRowIndexTransformer('idx', StartFrom::ZERO);
        $loader = to_memory(new ArrayMemory());

        $roots = (new Transformed($transformer, $loader))->roots($dataFrame->fork());

        static::assertCount(1, $roots);
        static::assertInstanceOf(Write::class, $roots[0]);
        static::assertSame($loader, $roots[0]->loader);
        $transform = $roots[0]->children()[0];
        static::assertInstanceOf(Transform::class, $transform);
        static::assertSame($transformer, $transform->transformer);
        static::assertSame([$dataFrame->cursor()], $transform->children());
    }

    public function test_a_transformation_builds_through_the_frames_verbs(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));

        $select = (new Transformed(select('id'), to_memory(new ArrayMemory())))->roots(
            $dataFrame->fork(),
        )[0]->children()[0];

        static::assertInstanceOf(Select::class, $select);
        static::assertSame([$dataFrame->cursor()], $select->children());
    }

    public function test_a_sink_child_extends_the_chain(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));

        $filter = (new Transformed(
            new AddRowIndexTransformer('idx', StartFrom::ZERO),
            new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
        ))->roots($dataFrame->fork())[0]->children()[0];

        static::assertInstanceOf(Filter::class, $filter);
        $transform = $filter->children()[0];
        static::assertInstanceOf(Transform::class, $transform);
        static::assertSame([$dataFrame->cursor()], $transform->children());
    }
}
