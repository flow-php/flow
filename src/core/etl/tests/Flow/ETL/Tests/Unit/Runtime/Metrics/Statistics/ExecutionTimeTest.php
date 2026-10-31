<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Metrics\Statistics;

use DateTimeImmutable;
use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\Runtime\Metrics\Statistics\ExecutionTime;
use Flow\ETL\Tests\FlowTestCase;

final class ExecutionTimeTest extends FlowTestCase
{
    public function test_normalize(): void
    {
        static::assertSame(
            [
                'started_at' => '2025-01-01T00:00:00.000+00:00',
                'finished_at' => '2025-01-01T00:05:00.250+00:00',
                'duration_ns' => 300_250_000_000,
            ],
            (new ExecutionTime(
                new DateTimeImmutable('2025-01-01 00:00:00 UTC'),
                new DateTimeImmutable('2025-01-01 00:05:00.250 UTC'),
                new HighResolutionTime(300, 250_000_000),
            ))->normalize(),
        );
    }
}
