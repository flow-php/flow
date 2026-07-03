<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\SemConvMetrics;
use PHPUnit\Framework\TestCase;

final class SemConvMetricsTest extends TestCase
{
    public function test_db_metric_names_follow_otel_convention(): void
    {
        static::assertSame('db.client.operation.duration', SemConvMetrics::DB_CLIENT_OPERATION_DURATION);
        static::assertSame('db.client.response.returned_rows', SemConvMetrics::DB_CLIENT_RESPONSE_RETURNED_ROWS);
    }

    public function test_messaging_metric_names_follow_otel_convention(): void
    {
        static::assertSame('messaging.client.consumed.messages', SemConvMetrics::MESSAGING_CLIENT_CONSUMED_MESSAGES);
        static::assertSame('messaging.client.sent.messages', SemConvMetrics::MESSAGING_CLIENT_SENT_MESSAGES);
        static::assertSame('messaging.process.duration', SemConvMetrics::MESSAGING_PROCESS_DURATION);
    }
}
