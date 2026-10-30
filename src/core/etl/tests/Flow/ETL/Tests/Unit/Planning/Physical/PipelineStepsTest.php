<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\RenameColumnTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Tests\FlowTestCase;

final class PipelineStepsTest extends FlowTestCase
{
    public function test_from_a_step_keeps_it_the_steps_after_it_and_the_breaker(): void
    {
        $first = new LimitTransformer(1);
        $offset = new OffsetProcessor(1);
        $collect = new CollectingProcessor();

        static::assertEquals(
            new PipelineSteps([$offset], $collect),
            (new PipelineSteps([$first, $offset], $collect))->from($offset),
        );
    }

    public function test_from_the_breaker_keeps_only_the_breaker(): void
    {
        $collect = new CollectingProcessor();

        static::assertEquals(
            new PipelineSteps([], $collect),
            (new PipelineSteps([new LimitTransformer(1)], $collect))->from($collect),
        );
    }

    public function test_from_null_leaves_nothing(): void
    {
        static::assertTrue(
            (new PipelineSteps([new LimitTransformer(1)], new CollectingProcessor()))
                ->from(null)
                ->isEmpty(),
        );
    }

    public function test_steps_with_only_a_breaker_are_not_empty(): void
    {
        static::assertFalse((new PipelineSteps([], new CollectingProcessor()))->isEmpty());
    }

    public function test_followed_by_finds_a_step_of_a_class_only_after_the_given_step(): void
    {
        $first = new RenameColumnTransformer('a', 'b');
        $limit = new LimitTransformer(1);
        $second = new RenameColumnTransformer('b', 'c');
        $steps = new PipelineSteps([$first, $limit, $second]);

        static::assertTrue($steps->followedBy($first, UntilTransformer::class, LimitTransformer::class));
        static::assertFalse($steps->followedBy($second, LimitTransformer::class));
        static::assertFalse($steps->followedBy(new RenameColumnTransformer('x', 'y'), LimitTransformer::class));
    }
}
