<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;

final class BranchedTest extends FlowTestCase
{
    public function test_the_condition_becomes_a_write_over_a_filter(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $loader = to_memory(new ArrayMemory());

        $roots = (new Branched(ref('id')->equals(lit(1)), $loader))->roots($dataFrame->fork());

        static::assertCount(1, $roots);
        static::assertInstanceOf(Write::class, $roots[0]);
        static::assertSame($loader, $roots[0]->loader);
        $filter = $roots[0]->children()[0];
        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame([$dataFrame->cursor()], $filter->children());
    }

    public function test_with_transformation_nests_a_transformed_sink(): void
    {
        $condition = ref('id')->equals(lit(1));
        $transformation = select('id');
        $loader = to_memory(new ArrayMemory());

        static::assertEquals(
            new Branched($condition, new Transformed($transformation, $loader)),
            (new Branched($condition, $loader))->withTransformation($transformation),
        );
    }

    public function test_with_transformation_filters_before_it_transforms(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));

        $select = (new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())))
            ->withTransformation(select('id'))
            ->roots($dataFrame->fork())[0]->children()[0];

        static::assertInstanceOf(Select::class, $select);
        $filter = $select->children()[0];
        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame([$dataFrame->cursor()], $filter->children());
    }
}
