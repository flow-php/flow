<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Tracer\{SpanStatus, SpanStatusCode};
use PHPUnit\Framework\TestCase;

final class SpanStatusTest extends TestCase
{
    public function test_constructor_creates_status_with_code_and_description() : void
    {
        $status = new SpanStatus(SpanStatusCode::ERROR, 'Something went wrong');

        self::assertSame(SpanStatusCode::ERROR, $status->code);
        self::assertSame('Something went wrong', $status->description);
    }

    public function test_constructor_creates_status_with_null_description() : void
    {
        $status = new SpanStatus(SpanStatusCode::OK);

        self::assertSame(SpanStatusCode::OK, $status->code);
        self::assertNull($status->description);
    }

    public function test_error_creates_error_status_with_description() : void
    {
        $status = SpanStatus::error('Connection failed');

        self::assertSame(SpanStatusCode::ERROR, $status->code);
        self::assertSame('Connection failed', $status->description);
        self::assertTrue($status->isError());
        self::assertFalse($status->isOk());
        self::assertFalse($status->isUnset());
    }

    public function test_error_creates_error_status_without_description() : void
    {
        $status = SpanStatus::error();

        self::assertSame(SpanStatusCode::ERROR, $status->code);
        self::assertNull($status->description);
        self::assertTrue($status->isError());
    }

    public function test_from_array_creates_status() : void
    {
        $data = ['code' => 2, 'description' => 'Test error'];
        $status = SpanStatus::fromArray($data);

        self::assertSame(SpanStatusCode::ERROR, $status->code);
        self::assertSame('Test error', $status->description);
    }

    public function test_from_array_creates_status_with_null_description() : void
    {
        $data = ['code' => 1, 'description' => null];
        $status = SpanStatus::fromArray($data);

        self::assertSame(SpanStatusCode::OK, $status->code);
        self::assertNull($status->description);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $original = SpanStatus::error('Original error');
        $normalized = $original->normalize();
        $restored = SpanStatus::fromArray($normalized);

        self::assertSame($original->code, $restored->code);
        self::assertSame($original->description, $restored->description);
    }

    public function test_normalize_returns_array() : void
    {
        $status = SpanStatus::error('Test description');

        self::assertSame([
            'code' => 2,
            'description' => 'Test description',
        ], $status->normalize());
    }

    public function test_ok_creates_ok_status() : void
    {
        $status = SpanStatus::ok();

        self::assertSame(SpanStatusCode::OK, $status->code);
        self::assertNull($status->description);
        self::assertTrue($status->isOk());
        self::assertFalse($status->isError());
        self::assertFalse($status->isUnset());
    }

    public function test_unset_creates_unset_status() : void
    {
        $status = SpanStatus::unset();

        self::assertSame(SpanStatusCode::UNSET, $status->code);
        self::assertNull($status->description);
        self::assertTrue($status->isUnset());
        self::assertFalse($status->isOk());
        self::assertFalse($status->isError());
    }
}
