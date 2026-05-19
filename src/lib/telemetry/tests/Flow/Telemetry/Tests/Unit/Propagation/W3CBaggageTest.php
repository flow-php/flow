<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\W3CBaggage;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class W3CBaggageTest extends TestCase
{
    public static function provideValidBaggageHeaders(): Generator
    {
        yield 'single entry' => [
            'key=value',
            ['key' => 'value'],
        ];
        yield 'multiple entries' => [
            'key1=value1,key2=value2',
            ['key1' => 'value1', 'key2' => 'value2'],
        ];
        yield 'entries with spaces' => [
            ' key1=value1 , key2=value2 ',
            ['key1' => 'value1', 'key2' => 'value2'],
        ];
        yield 'entry with properties (ignored)' => [
            'key=value;property1;property2=propValue',
            ['key' => 'value'],
        ];
        yield 'url encoded key and value' => [
            'user%2Did=123%3A456',
            ['user-id' => '123:456'],
        ];
    }

    public function test_extract_ignores_empty_keys(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => '=value,key=value']);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame(['key' => 'value'], $ctx->baggage->all());
    }

    public function test_extract_ignores_invalid_entries(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => 'valid=value,invalid,also=valid']);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame('value', $ctx->baggage->get('valid'));
        static::assertSame('valid', $ctx->baggage->get('also'));
        static::assertNull($ctx->baggage->get('invalid'));
    }

    /**
     * @param array<string, string> $expected
     */
    #[DataProvider('provideValidBaggageHeaders')]
    public function test_extract_parses_valid_baggage(string $header, array $expected): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => $header]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame($expected, $ctx->baggage->all());
    }

    public function test_extract_returns_empty_baggage_for_empty_header(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => '']);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_extract_returns_empty_baggage_for_missing_header(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier([]);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_extract_returns_propagation_context_without_span_context(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => 'key=value']);

        $ctx = $propagator->extract($carrier);

        static::assertNull($ctx->spanContext);
        static::assertNotNull($ctx->baggage);
    }

    public function test_extract_with_case_insensitive_headers(): void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['Baggage' => 'key=value']);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_fields_returns_correct_header_name(): void
    {
        $propagator = new W3CBaggage();

        static::assertSame(['baggage'], $propagator->fields());
    }

    public function test_inject_does_nothing_for_empty_baggage(): void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage());
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertArrayNotHasKey('baggage', $carrier->unwrap());
    }

    public function test_inject_does_nothing_for_null_baggage(): void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext();
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertArrayNotHasKey('baggage', $carrier->unwrap());
    }

    public function test_inject_multiple_entries(): void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['key1' => 'value1', 'key2' => 'value2']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        static::assertArrayHasKey('baggage', $headers);
        $baggageHeader = $headers['baggage'];
        static::assertIsString($baggageHeader);
        static::assertStringContainsString('key1=value1', $baggageHeader);
        static::assertStringContainsString('key2=value2', $baggageHeader);
        static::assertStringContainsString(',', $baggageHeader);
    }

    public function test_inject_sets_baggage_header(): void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['key' => 'value']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertSame('key=value', $carrier->unwrap()['baggage']);
    }

    public function test_inject_url_encodes_key_and_value(): void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['user-id' => '123:456']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        static::assertSame('user-id=123%3A456', $carrier->unwrap()['baggage']);
    }

    public function test_round_trip_preserves_baggage(): void
    {
        $propagator = new W3CBaggage();
        $original = new PropagationContext(baggage: new Baggage([
            'user.id' => '12345',
            'session.id' => 'abc-def',
        ]));
        $carrier = new ArrayCarrier();

        $propagator->inject($original, $carrier);
        $restored = $propagator->extract(new ArrayCarrier($carrier->unwrap()));

        $restoredBaggage = $restored->baggage;
        $originalBaggage = $original->baggage;
        static::assertNotNull($restoredBaggage);
        static::assertNotNull($originalBaggage);
        static::assertSame($originalBaggage->get('user.id'), $restoredBaggage->get('user.id'));
        static::assertSame($originalBaggage->get('session.id'), $restoredBaggage->get('session.id'));
    }
}
