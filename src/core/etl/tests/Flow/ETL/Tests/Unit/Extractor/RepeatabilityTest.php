<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\Repeatability;
use Flow\ETL\Tests\Double\EmptyExtractor;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Tests\FlowTestCase;

final class RepeatabilityTest extends FlowTestCase
{
    public function test_an_extractor_that_does_not_declare_the_capability_is_refused(): void
    {
        static::assertFalse((new Repeatability())->of(new EmptyExtractor()));
    }

    public function test_a_wrapper_is_refused_when_a_nested_extractor_is(): void
    {
        static::assertFalse((new Repeatability())->of(
            new RepeatableExtractor(true, new RepeatableExtractor(true, new RepeatableExtractor(false))),
        ));
    }

    public function test_a_wrapper_is_refused_when_its_inner_extractor_is(): void
    {
        static::assertFalse((new Repeatability())->of(new RepeatableExtractor(true, new EmptyExtractor())));
    }

    public function test_a_wrapper_whose_children_all_repeat_repeats(): void
    {
        static::assertTrue((new Repeatability())->of(
            new RepeatableExtractor(true, new RepeatableExtractor(true), new RepeatableExtractor(true)),
        ));
    }

    public function test_it_answers_the_extractors_own_verdict(): void
    {
        static::assertTrue((new Repeatability())->of(new RepeatableExtractor(true)));
        static::assertFalse((new Repeatability())->of(new RepeatableExtractor(false)));
    }
}
