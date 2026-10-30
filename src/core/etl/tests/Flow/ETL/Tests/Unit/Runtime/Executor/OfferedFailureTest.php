<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\LoadingAction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationAction;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Runtime\Executor\OfferedFailure;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BranchLoaderMother;
use RuntimeException;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class OfferedFailureTest extends FlowTestCase
{
    public function test_nothing_is_offered_before_the_first_failure(): void
    {
        static::assertNull((new OfferedFailure(null))->undecided(new RuntimeException('boom')));
    }

    public function test_an_extraction_failure_is_left_to_the_handler_and_not_remembered(): void
    {
        $offers = new OfferedFailure(null);
        $cause = new RuntimeException('boom');

        static::assertSame(
            ExtractionAction::propagate,
            $offers->onExtraction(new ExtractionError($cause, from_array([['id' => 1]]))),
        );
        static::assertNull($offers->undecided($cause));
    }

    public function test_an_extraction_failure_is_decided_by_the_handler(): void
    {
        $offers = new OfferedFailure(new IgnoreError());
        $cause = new RuntimeException('boom');

        static::assertSame(
            ExtractionAction::endSource,
            $offers->onExtraction(new ExtractionError($cause, from_array([['id' => 1]]))),
        );
        static::assertNull($offers->undecided($cause));
    }

    public function test_an_undecided_transformation_failure_propagates_and_is_remembered(): void
    {
        $offers = new OfferedFailure(null);
        $error = new TransformationError(
            new RuntimeException('boom'),
            new SpyTransformer(),
            BranchLoaderMother::batch(),
        );

        static::assertSame(TransformationAction::propagate, $offers->onTransformation($error));
        static::assertSame($error, $offers->undecided($error->cause));
    }

    public function test_an_undecided_loading_failure_propagates_and_is_remembered(): void
    {
        $offers = new OfferedFailure(null);
        $error = new LoadingError(
            new RuntimeException('boom'),
            to_memory(new ArrayMemory()),
            BranchLoaderMother::batch(),
        );

        static::assertSame(LoadingAction::propagate, $offers->onLoading($error));
        static::assertSame($error, $offers->undecided($error->cause));
    }

    public function test_a_failure_a_handler_decided_is_not_undecided(): void
    {
        $offers = new OfferedFailure(new IgnoreError());
        $transformation = new TransformationError(
            new RuntimeException('boom'),
            new SpyTransformer(),
            BranchLoaderMother::batch(),
        );
        $loading = new LoadingError(
            new RuntimeException('boom'),
            to_memory(new ArrayMemory()),
            BranchLoaderMother::batch(),
        );

        static::assertSame(TransformationAction::skipBatch, $offers->onTransformation($transformation));
        static::assertNull($offers->undecided($transformation->cause));
        static::assertSame(LoadingAction::skipLoader, $offers->onLoading($loading));
        static::assertNull($offers->undecided($loading->cause));
    }

    public function test_undecided_compares_by_identity(): void
    {
        $offers = new OfferedFailure(null);
        $offers->onLoading(
            new LoadingError(new RuntimeException('boom'), to_memory(new ArrayMemory()), BranchLoaderMother::batch()),
        );

        static::assertNull($offers->undecided(new RuntimeException('boom')));
    }

    public function test_forget_clears_the_last_offer(): void
    {
        $offers = new OfferedFailure(null);
        $cause = new RuntimeException('boom');
        $offers->onLoading(new LoadingError($cause, to_memory(new ArrayMemory()), BranchLoaderMother::batch()));

        $offers->forget();

        static::assertNull($offers->undecided($cause));
    }
}
