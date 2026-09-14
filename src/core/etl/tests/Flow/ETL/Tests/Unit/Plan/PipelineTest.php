<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Extractor\Scan;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\from_array;

final class PipelineTest extends FlowTestCase
{
    public function test_accessors_return_the_values_it_was_built_with(): void
    {
        $segments = new Segments();
        $context = NodeMother::context();
        $input = new Pipeline(0, new Segments(from_array([['id' => 1]])), $context);
        $frame = new Pipeline(0, new Segments(from_array([['id' => 2]])), $context);
        $scan = new Scan(limit: 3);

        $pipeline = new Pipeline(1, $segments, $context, $input, [$frame], $scan);

        static::assertSame(1, $pipeline->id);
        static::assertSame($segments, $pipeline->segments());
        static::assertSame($context, $pipeline->context());
        static::assertSame($input, $pipeline->input());
        static::assertSame([$frame], $pipeline->frames());
        static::assertSame($scan, $pipeline->scan());
    }

    public function test_a_leaf_pipeline_defaults_to_no_input_no_frames_and_an_empty_scan(): void
    {
        $pipeline = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());

        static::assertNull($pipeline->input());
        static::assertSame([], $pipeline->frames());
        static::assertNull($pipeline->scan()->limit);
        static::assertInstanceOf(OnlyFiles::class, $pipeline->scan()->pathFilter);
    }

    public function test_dependencies_is_the_input_followed_by_the_frames(): void
    {
        $input = new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context());
        $frame = new Pipeline(0, new Segments(from_array([['id' => 2]])), NodeMother::context());

        static::assertSame(
            [$input, $frame],
            (new Pipeline(1, new Segments(), NodeMother::context(), $input, [$frame]))->dependencies(),
        );
    }

    public function test_dependencies_is_empty_for_a_leaf_pipeline_with_no_frames(): void
    {
        static::assertSame(
            [],
            (new Pipeline(0, new Segments(from_array([['id' => 1]])), NodeMother::context()))->dependencies(),
        );
    }

    public function test_dependencies_omits_a_null_input(): void
    {
        $frame = new Pipeline(0, new Segments(from_array([['id' => 2]])), NodeMother::context());

        static::assertSame(
            [$frame],
            (new Pipeline(0, new Segments(), NodeMother::context(), null, [$frame]))->dependencies(),
        );
    }
}
