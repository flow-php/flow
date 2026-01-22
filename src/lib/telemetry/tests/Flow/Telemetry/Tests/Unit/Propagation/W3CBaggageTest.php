<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Propagation\{ArrayCarrier, PropagationContext, W3CBaggage};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class W3CBaggageTest extends TestCase
{
    public static function provideValidBaggageHeaders() : \Generator
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

    public function test_extract_ignores_empty_keys() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => '=value,key=value']);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertSame(['key' => 'value'], $ctx->baggage->all());
    }

    public function test_extract_ignores_invalid_entries() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => 'valid=value,invalid,also=valid']);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertSame('value', $ctx->baggage->get('valid'));
        self::assertSame('valid', $ctx->baggage->get('also'));
        self::assertNull($ctx->baggage->get('invalid'));
    }

    /**
     * @param array<string, string> $expected
     */
    #[DataProvider('provideValidBaggageHeaders')]
    public function test_extract_parses_valid_baggage(string $header, array $expected) : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => $header]);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertSame($expected, $ctx->baggage->all());
    }

    public function test_extract_returns_empty_baggage_for_empty_header() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => '']);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_extract_returns_empty_baggage_for_missing_header() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier([]);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertTrue($ctx->baggage->isEmpty());
    }

    public function test_extract_returns_propagation_context_without_span_context() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['baggage' => 'key=value']);

        $ctx = $propagator->extract($carrier);

        self::assertNull($ctx->spanContext);
        self::assertNotNull($ctx->baggage);
    }

    public function test_extract_with_case_insensitive_headers() : void
    {
        $propagator = new W3CBaggage();
        $carrier = new ArrayCarrier(['Baggage' => 'key=value']);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertSame('value', $ctx->baggage->get('key'));
    }

    public function test_fields_returns_correct_header_name() : void
    {
        $propagator = new W3CBaggage();

        self::assertSame(['baggage'], $propagator->fields());
    }

    public function test_inject_does_nothing_for_empty_baggage() : void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage());
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        self::assertArrayNotHasKey('baggage', $carrier->unwrap());
    }

    public function test_inject_does_nothing_for_null_baggage() : void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext();
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        self::assertArrayNotHasKey('baggage', $carrier->unwrap());
    }

    public function test_inject_multiple_entries() : void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['key1' => 'value1', 'key2' => 'value2']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        $headers = $carrier->unwrap();
        self::assertArrayHasKey('baggage', $headers);
        $baggageHeader = $headers['baggage'];
        self::assertIsString($baggageHeader);
        self::assertStringContainsString('key1=value1', $baggageHeader);
        self::assertStringContainsString('key2=value2', $baggageHeader);
        self::assertStringContainsString(',', $baggageHeader);
    }

    public function test_inject_sets_baggage_header() : void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['key' => 'value']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        self::assertSame('key=value', $carrier->unwrap()['baggage']);
    }

    public function test_inject_url_encodes_key_and_value() : void
    {
        $propagator = new W3CBaggage();
        $ctx = new PropagationContext(baggage: new Baggage(['user-id' => '123:456']));
        $carrier = new ArrayCarrier();

        $propagator->inject($ctx, $carrier);

        self::assertSame('user-id=123%3A456', $carrier->unwrap()['baggage']);
    }

    public function test_round_trip_preserves_baggage() : void
    {
        $propagator = new W3CBaggage();
        $original = new PropagationContext(baggage: new Baggage([
            'user.id' => '12345',
            'session.id' => 'abc-def',
        ]));
        $carrier = new ArrayCarrier();

        $propagator->inject($original, $carrier);
        $restored = $propagator->extract(new ArrayCarrier($carrier->unwrap()));

        self::assertNotNull($restored->baggage);
        self::assertNotNull($original->baggage);
        self::assertSame($original->baggage->get('user.id'), $restored->baggage->get('user.id'));
        self::assertSame($original->baggage->get('session.id'), $restored->baggage->get('session.id'));
    }
}
