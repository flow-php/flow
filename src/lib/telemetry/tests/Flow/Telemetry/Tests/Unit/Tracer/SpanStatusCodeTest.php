<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\SpanStatusCode;
use PHPUnit\Framework\TestCase;

final class SpanStatusCodeTest extends TestCase
{
    public function test_all_cases_exist() : void
    {
        $cases = SpanStatusCode::cases();

        self::assertCount(3, $cases);
    }

    public function test_error_has_correct_value() : void
    {
        self::assertSame(2, SpanStatusCode::ERROR->value);
    }

    public function test_ok_has_correct_value() : void
    {
        self::assertSame(1, SpanStatusCode::OK->value);
    }

    public function test_unset_has_correct_value() : void
    {
        self::assertSame(0, SpanStatusCode::UNSET->value);
    }
}
