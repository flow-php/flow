<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\SpanStatusCode;
use PHPUnit\Framework\TestCase;

final class SpanStatusTest extends TestCase
{
    public function test_constructor_creates_status_with_code_and_description(): void
    {
        $status = new SpanStatus(SpanStatusCode::ERROR, 'Something went wrong');

        static::assertSame(SpanStatusCode::ERROR, $status->code);
        static::assertSame('Something went wrong', $status->description);
    }

    public function test_constructor_creates_status_with_null_description(): void
    {
        $status = new SpanStatus(SpanStatusCode::OK);

        static::assertSame(SpanStatusCode::OK, $status->code);
        static::assertNull($status->description);
    }

    public function test_error_creates_error_status_with_description(): void
    {
        $status = SpanStatus::error('Connection failed');

        static::assertSame(SpanStatusCode::ERROR, $status->code);
        static::assertSame('Connection failed', $status->description);
        static::assertTrue($status->isError());
        static::assertFalse($status->isOk());
        static::assertFalse($status->isUnset());
    }

    public function test_error_creates_error_status_without_description(): void
    {
        $status = SpanStatus::error();

        static::assertSame(SpanStatusCode::ERROR, $status->code);
        static::assertNull($status->description);
        static::assertTrue($status->isError());
    }

    public function test_from_array_creates_status(): void
    {
        $data = ['code' => 2, 'description' => 'Test error'];
        $status = SpanStatus::fromArray($data);

        static::assertSame(SpanStatusCode::ERROR, $status->code);
        static::assertSame('Test error', $status->description);
    }

    public function test_from_array_creates_status_with_null_description(): void
    {
        $data = ['code' => 1, 'description' => null];
        $status = SpanStatus::fromArray($data);

        static::assertSame(SpanStatusCode::OK, $status->code);
        static::assertNull($status->description);
    }

    public function test_normalize_from_array_round_trip(): void
    {
        $original = SpanStatus::error('Original error');
        $normalized = $original->normalize();
        $restored = SpanStatus::fromArray($normalized);

        static::assertSame($original->code, $restored->code);
        static::assertSame($original->description, $restored->description);
    }

    public function test_normalize_returns_array(): void
    {
        $status = SpanStatus::error('Test description');

        static::assertSame(
            [
                'code' => 2,
                'description' => 'Test description',
            ],
            $status->normalize(),
        );
    }

    public function test_ok_creates_ok_status(): void
    {
        $status = SpanStatus::ok();

        static::assertSame(SpanStatusCode::OK, $status->code);
        static::assertNull($status->description);
        static::assertTrue($status->isOk());
        static::assertFalse($status->isError());
        static::assertFalse($status->isUnset());
    }

    public function test_unset_creates_unset_status(): void
    {
        $status = SpanStatus::unset();

        static::assertSame(SpanStatusCode::UNSET, $status->code);
        static::assertNull($status->description);
        static::assertTrue($status->isUnset());
        static::assertFalse($status->isOk());
        static::assertFalse($status->isError());
    }
}
