<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\{TelemetryStamp, TelemetryStampCarrier};
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(TelemetryStampCarrier::class)]
final class TelemetryStampCarrierTest extends TestCase
{
    public function test_constructs_with_empty_stamp_by_default() : void
    {
        $carrier = new TelemetryStampCarrier();

        $stamp = $carrier->unwrap();

        self::assertInstanceOf(TelemetryStamp::class, $stamp);
        self::assertSame([], $stamp->all());
    }

    public function test_constructs_with_provided_stamp() : void
    {
        $stamp = new TelemetryStamp(['traceparent' => 'value']);

        $carrier = new TelemetryStampCarrier($stamp);

        self::assertSame('value', $carrier->unwrap()->get('traceparent'));
    }

    public function test_get_delegates_to_underlying_stamp() : void
    {
        $stamp = new TelemetryStamp(['traceparent' => 'value', 'tracestate' => 'vendor=data']);
        $carrier = new TelemetryStampCarrier($stamp);

        self::assertSame('value', $carrier->get('traceparent'));
        self::assertSame('vendor=data', $carrier->get('tracestate'));
    }

    public function test_get_returns_null_for_missing_key() : void
    {
        $carrier = new TelemetryStampCarrier();

        self::assertNull($carrier->get('nonexistent'));
    }

    public function test_set_can_be_chained_multiple_times() : void
    {
        $carrier = new TelemetryStampCarrier();

        $carrier
            ->set('traceparent', 'trace-value')
            ->set('tracestate', 'state-value')
            ->set('baggage', 'baggage-value');

        self::assertSame('trace-value', $carrier->get('traceparent'));
        self::assertSame('state-value', $carrier->get('tracestate'));
        self::assertSame('baggage-value', $carrier->get('baggage'));
    }

    public function test_set_returns_same_carrier_instance_for_chaining() : void
    {
        $carrier = new TelemetryStampCarrier();

        $result = $carrier->set('traceparent', 'value');

        self::assertSame($carrier, $result);
    }

    public function test_set_updates_underlying_stamp() : void
    {
        $carrier = new TelemetryStampCarrier();

        $carrier->set('traceparent', 'value');

        self::assertSame('value', $carrier->get('traceparent'));
    }

    public function test_unwrap_returns_current_stamp() : void
    {
        $originalStamp = new TelemetryStamp(['key' => 'original']);
        $carrier = new TelemetryStampCarrier($originalStamp);

        $stamp = $carrier->unwrap();

        self::assertSame('original', $stamp->get('key'));
    }

    public function test_unwrap_returns_stamp_with_all_set_values() : void
    {
        $carrier = new TelemetryStampCarrier();

        $carrier->set('traceparent', 'trace');
        $carrier->set('tracestate', 'state');

        $stamp = $carrier->unwrap();

        self::assertSame('trace', $stamp->get('traceparent'));
        self::assertSame('state', $stamp->get('tracestate'));
        self::assertCount(2, $stamp->all());
    }
}
