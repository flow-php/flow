<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry\Tests\Unit;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\Exception\RuntimeException;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\RequestCarrier;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

#[CoversClass(RequestCarrier::class)]
final class RequestCarrierTest extends TestCase
{
    public function test_extract_baggage_from_request() : void
    {
        $request = new Request();
        $request->headers->set('baggage', 'userId=alice,serverNode=DF28');

        $carrier = new RequestCarrier($request);

        self::assertSame('userId=alice,serverNode=DF28', $carrier->get('baggage'));
    }

    public function test_extract_traceparent_from_request() : void
    {
        $request = new Request();
        $request->headers->set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01');

        $carrier = new RequestCarrier($request);

        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $carrier->get('traceparent'),
        );
    }

    public function test_get_is_case_insensitive() : void
    {
        $request = new Request();
        $request->headers->set('X-Custom-Header', 'custom-value');

        $carrier = new RequestCarrier($request);

        self::assertSame('custom-value', $carrier->get('x-custom-header'));
        self::assertSame('custom-value', $carrier->get('X-CUSTOM-HEADER'));
        self::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_returns_header_value() : void
    {
        $request = new Request();
        $request->headers->set('X-Custom-Header', 'custom-value');

        $carrier = new RequestCarrier($request);

        self::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_returns_null_for_missing_header() : void
    {
        $request = new Request();

        $carrier = new RequestCarrier($request);

        self::assertNull($carrier->get('nonexistent'));
    }

    public function test_set_throws_runtime_exception() : void
    {
        $request = new Request();

        $carrier = new RequestCarrier($request);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('RequestCarrier is read-only');

        $carrier->set('key', 'value');
    }

    public function test_unwrap_returns_original_request() : void
    {
        $request = new Request();
        $request->headers->set('X-Custom-Header', 'custom-value');

        $carrier = new RequestCarrier($request);

        self::assertSame($request, $carrier->unwrap());
    }
}
