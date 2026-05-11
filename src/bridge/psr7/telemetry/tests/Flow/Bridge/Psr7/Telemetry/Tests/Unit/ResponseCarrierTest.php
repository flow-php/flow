<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry\Tests\Unit;

use Flow\Bridge\Psr7\Telemetry\ResponseCarrier;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\TestCase;

final class ResponseCarrierTest extends TestCase
{
    public function test_fluent_chaining(): void
    {
        $response = new Response();

        $modifiedResponse = (new ResponseCarrier($response))
            ->set('X-Header-1', 'value1')
            ->set('X-Header-2', 'value2')
            ->unwrap();

        static::assertSame('value1', $modifiedResponse->getHeader('X-Header-1')[0]);
        static::assertSame('value2', $modifiedResponse->getHeader('X-Header-2')[0]);
    }

    public function test_get_is_case_insensitive(): void
    {
        $response = new Response(200, [
            'X-Custom-Header' => 'custom-value',
        ]);

        $carrier = new ResponseCarrier($response);

        static::assertSame('custom-value', $carrier->get('x-custom-header'));
        static::assertSame('custom-value', $carrier->get('X-CUSTOM-HEADER'));
        static::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_returns_header_value(): void
    {
        $response = new Response(200, [
            'X-Custom-Header' => 'custom-value',
        ]);

        $carrier = new ResponseCarrier($response);

        static::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_returns_null_for_missing_header(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);

        static::assertNull($carrier->get('nonexistent'));
    }

    public function test_inject_baggage_into_response(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);
        $carrier->set('baggage', 'userId=alice,serverNode=DF28');

        static::assertSame('userId=alice,serverNode=DF28', $carrier->unwrap()->getHeader('baggage')[0]);
    }

    public function test_inject_traceparent_into_response(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);
        $carrier->set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01');

        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $carrier->unwrap()->getHeader('traceparent')[0],
        );
    }

    public function test_set_adds_header_to_response(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);
        $carrier->set('X-Custom-Header', 'custom-value');

        static::assertSame('custom-value', $carrier->unwrap()->getHeader('X-Custom-Header')[0]);
    }

    public function test_set_overwrites_existing_header(): void
    {
        $response = new Response(200, [
            'X-Custom-Header' => 'old-value',
        ]);

        $carrier = new ResponseCarrier($response);
        $carrier->set('X-Custom-Header', 'new-value');

        static::assertSame('new-value', $carrier->get('X-Custom-Header'));
    }

    public function test_set_returns_self_for_fluent_chaining(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);

        $result = $carrier->set('X-Custom-Header', 'custom-value');

        static::assertSame($carrier, $result);
    }

    public function test_unwrap_returns_modified_response(): void
    {
        $response = new Response();

        $carrier = new ResponseCarrier($response);
        $carrier->set('X-Custom-Header', 'custom-value');

        $modifiedResponse = $carrier->unwrap();

        static::assertSame('custom-value', $modifiedResponse->getHeader('X-Custom-Header')[0]);
    }
}
