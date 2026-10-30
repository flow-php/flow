<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\ErrorHandler;

use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ThrowErrorTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        static::assertSame(
            ExtractionAction::propagate,
            DescribeRoundTrip::rebuilt(new ThrowError())->onExtraction(
                new ExtractionError(new RuntimeException('x'), from_rows(rows(schema()))),
            ),
        );
    }
}
