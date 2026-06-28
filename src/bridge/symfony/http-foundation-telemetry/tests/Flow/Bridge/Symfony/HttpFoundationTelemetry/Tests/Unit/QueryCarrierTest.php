<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry\Tests\Unit;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\Exception\RuntimeException;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\QueryCarrier;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

#[CoversClass(QueryCarrier::class)]
final class QueryCarrierTest extends TestCase
{
    public function test_get_returns_query_parameter(): void
    {
        $request = Request::create('/next?traceparent=00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01');

        $carrier = new QueryCarrier($request);

        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $carrier->get('traceparent'));
    }

    public function test_get_returns_null_for_missing_parameter(): void
    {
        $carrier = new QueryCarrier(Request::create('/next'));

        static::assertNull($carrier->get('traceparent'));
    }

    public function test_get_returns_null_for_non_string_parameter(): void
    {
        $carrier = new QueryCarrier(Request::create('/next?traceparent[]=a&traceparent[]=b'));

        static::assertNull($carrier->get('traceparent'));
    }

    public function test_set_throws_runtime_exception(): void
    {
        $carrier = new QueryCarrier(Request::create('/next'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('QueryCarrier is read-only');

        $carrier->set('key', 'value');
    }

    public function test_unwrap_returns_original_request(): void
    {
        $request = Request::create('/next?traceparent=abc');

        $carrier = new QueryCarrier($request);

        static::assertSame($request, $carrier->unwrap());
    }
}
