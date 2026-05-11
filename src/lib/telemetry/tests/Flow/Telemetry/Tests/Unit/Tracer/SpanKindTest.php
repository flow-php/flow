<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\TestCase;

final class SpanKindTest extends TestCase
{
    public function test_all_cases_exist(): void
    {
        $cases = SpanKind::cases();

        static::assertCount(5, $cases);
    }

    public function test_client_has_correct_value(): void
    {
        static::assertSame('client', SpanKind::CLIENT->value);
    }

    public function test_consumer_has_correct_value(): void
    {
        static::assertSame('consumer', SpanKind::CONSUMER->value);
    }

    public function test_internal_has_correct_value(): void
    {
        static::assertSame('internal', SpanKind::INTERNAL->value);
    }

    public function test_producer_has_correct_value(): void
    {
        static::assertSame('producer', SpanKind::PRODUCER->value);
    }

    public function test_server_has_correct_value(): void
    {
        static::assertSame('server', SpanKind::SERVER->value);
    }
}
