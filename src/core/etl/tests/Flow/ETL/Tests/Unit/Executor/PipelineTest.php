<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

use function Flow\ETL\DSL\from_array;

final class PipelineTest extends FlowTestCase
{
    public function test_accessors_return_the_values_it_was_built_with(): void
    {
        $segments = new Segments();
        $context = NodeMother::context();
        $input = new Pipeline(0, new Segments(from_array([['id' => 1]])), $context);
        $filter = new RejectingFilter();

        $pipeline = new Pipeline(1, $segments, $context, $input, 3, $filter);

        static::assertSame(1, $pipeline->id);
        static::assertSame($segments, $pipeline->segments());
        static::assertSame($context, $pipeline->context());
        static::assertSame($input, $pipeline->input());
        static::assertSame(3, $pipeline->limit());
        static::assertSame($filter, $pipeline->pathFilter());
    }

    public function test_a_leaf_pipeline_defaults_to_no_input_no_limit_and_only_files(): void
    {
        $pipeline = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());

        static::assertNull($pipeline->input());
        static::assertNull($pipeline->limit());
        static::assertInstanceOf(OnlyFiles::class, $pipeline->pathFilter());
    }
}
