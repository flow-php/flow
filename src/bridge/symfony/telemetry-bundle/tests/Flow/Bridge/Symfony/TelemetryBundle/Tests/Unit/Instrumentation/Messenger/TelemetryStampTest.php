<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TelemetryStamp;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(TelemetryStamp::class)]
final class TelemetryStampTest extends TestCase
{
    public function test_all_returns_context_array_as_provided(): void
    {
        $context = [
            'traceparent' => '00-abcdef0123456789abcdef0123456789-0123456789abcdef-01',
            'tracestate' => 'vendor=value',
        ];

        $stamp = new TelemetryStamp($context);

        static::assertSame($context, $stamp->all());
    }

    public function test_all_returns_empty_array_when_constructed_without_context(): void
    {
        $stamp = new TelemetryStamp();

        static::assertSame([], $stamp->all());
    }

    public function test_get_is_case_insensitive_with_lowercase_lookup(): void
    {
        $stamp = new TelemetryStamp(['Traceparent' => 'value']);

        static::assertSame('value', $stamp->get('traceparent'));
    }

    public function test_get_is_case_insensitive_with_mixed_case_lookup(): void
    {
        $stamp = new TelemetryStamp(['TRACEPARENT' => 'value']);

        static::assertSame('value', $stamp->get('TraceParent'));
    }

    public function test_get_is_case_insensitive_with_uppercase_lookup(): void
    {
        $stamp = new TelemetryStamp(['traceparent' => 'value']);

        static::assertSame('value', $stamp->get('TRACEPARENT'));
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $stamp = new TelemetryStamp(['traceparent' => 'value']);

        static::assertNull($stamp->get('nonexistent'));
    }

    public function test_get_returns_value_for_exact_key_match(): void
    {
        $stamp = new TelemetryStamp([
            'traceparent' => '00-abcdef0123456789abcdef0123456789-0123456789abcdef-01',
        ]);

        static::assertSame('00-abcdef0123456789abcdef0123456789-0123456789abcdef-01', $stamp->get('traceparent'));
    }

    public function test_with_adds_new_key_and_returns_new_instance(): void
    {
        $original = new TelemetryStamp(['traceparent' => 'original']);

        $newStamp = $original->with('tracestate', 'vendor=value');

        static::assertNotSame($original, $newStamp);
        static::assertSame('vendor=value', $newStamp->get('tracestate'));
        static::assertSame('original', $newStamp->get('traceparent'));
    }

    public function test_with_does_not_modify_original_stamp(): void
    {
        $original = new TelemetryStamp(['traceparent' => 'original']);

        $original->with('tracestate', 'vendor=value');

        static::assertNull($original->get('tracestate'));
        static::assertSame(['traceparent' => 'original'], $original->all());
    }

    public function test_with_overwrites_existing_key(): void
    {
        $stamp = new TelemetryStamp(['traceparent' => 'original']);

        $newStamp = $stamp->with('traceparent', 'updated');

        static::assertSame('updated', $newStamp->get('traceparent'));
    }
}
