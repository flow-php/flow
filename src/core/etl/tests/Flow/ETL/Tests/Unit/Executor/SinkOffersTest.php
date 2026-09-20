<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;
use Flow\ETL\Executor\SinkOffers;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\SinkFeedMother;
use RuntimeException;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class SinkOffersTest extends FlowTestCase
{
    public function test_nothing_is_offered_before_the_first_failure(): void
    {
        static::assertFalse((new SinkOffers(new IgnoreError()))->offered(new RuntimeException('boom')));
    }

    public function test_an_extraction_failure_is_recorded_and_the_handler_decides(): void
    {
        $offers = new SinkOffers(new IgnoreError());
        $cause = new RuntimeException('boom');

        static::assertSame(
            ExtractionAction::endSource,
            $offers->onExtraction(new ExtractionError($cause, from_array([['id' => 1]]))),
        );
        static::assertTrue($offers->offered($cause));
    }

    public function test_a_transformation_failure_is_recorded_and_the_handler_decides(): void
    {
        $offers = new SinkOffers(new IgnoreError());
        $cause = new RuntimeException('boom');

        static::assertSame(
            TransformationAction::skipBatch,
            $offers->onTransformation(new TransformationError($cause, new SpyTransformer(), SinkFeedMother::batch())),
        );
        static::assertTrue($offers->offered($cause));
    }

    public function test_a_loading_failure_is_recorded_and_the_handler_decides(): void
    {
        $offers = new SinkOffers(new IgnoreError());
        $cause = new RuntimeException('boom');

        static::assertSame(
            LoadingAction::skipLoader,
            $offers->onLoading(new LoadingError($cause, to_memory(new ArrayMemory()), SinkFeedMother::batch())),
        );
        static::assertTrue($offers->offered($cause));
    }

    public function test_offered_compares_by_identity(): void
    {
        $offers = new SinkOffers(new IgnoreError());
        $offers->onLoading(
            new LoadingError(new RuntimeException('boom'), to_memory(new ArrayMemory()), SinkFeedMother::batch()),
        );

        static::assertFalse($offers->offered(new RuntimeException('boom')));
    }

    public function test_forget_clears_the_last_offer(): void
    {
        $offers = new SinkOffers(new IgnoreError());
        $cause = new RuntimeException('boom');
        $offers->onLoading(new LoadingError($cause, to_memory(new ArrayMemory()), SinkFeedMother::batch()));

        $offers->forget();

        static::assertFalse($offers->offered($cause));
    }
}
