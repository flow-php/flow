<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\ErrorHandler;

use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\skip_rows_handler;

final class SkipRowsTest extends FlowTestCase
{
    public function test_a_failing_source_ends(): void
    {
        static::assertSame(
            ExtractionAction::endSource,
            skip_rows_handler()->onExtraction(new ExtractionError(new RuntimeException(), from_rows())),
        );
    }

    public function test_a_failing_loader_stops_the_run(): void
    {
        static::assertSame(
            LoadingAction::propagate,
            skip_rows_handler()->onLoading(new LoadingError(new RuntimeException(), new SpyLoader(), rows(schema()))),
        );
    }

    public function test_a_failing_transformation_skips_its_batch(): void
    {
        static::assertSame(
            TransformationAction::skipBatch,
            skip_rows_handler()->onTransformation(
                new TransformationError(
                    new RuntimeException(),
                    new ThrowingTransformer(new RuntimeException()),
                    rows(schema()),
                ),
            ),
        );
    }
}
