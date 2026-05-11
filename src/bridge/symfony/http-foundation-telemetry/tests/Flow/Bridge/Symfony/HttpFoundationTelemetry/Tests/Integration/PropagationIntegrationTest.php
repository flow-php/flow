<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry\Tests\Integration;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\RequestCarrier;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\ResponseCarrier;
use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

#[CoversClass(RequestCarrier::class)]
#[CoversClass(ResponseCarrier::class)]
final class PropagationIntegrationTest extends TestCase
{
    public function test_extract_baggage_from_symfony_request(): void
    {
        $request = Request::create(
            '/api/users',
            'GET',
            [],
            [],
            [],
            [
                'HTTP_BAGGAGE' => 'userId=alice,serverNode=DF28,isProduction=false',
            ],
        );

        $propagator = new W3CBaggage();
        $carrier = new RequestCarrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame('alice', $ctx->baggage->get('userId'));
        static::assertSame('DF28', $ctx->baggage->get('serverNode'));
        static::assertSame('false', $ctx->baggage->get('isProduction'));
    }

    public function test_extract_trace_context_from_symfony_request(): void
    {
        $request = Request::create(
            '/api/users',
            'GET',
            [],
            [],
            [],
            [
                'HTTP_TRACEPARENT' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
                'HTTP_TRACESTATE' => 'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE',
            ],
        );

        $propagator = new W3CTraceContext();
        $carrier = new RequestCarrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $ctx->spanContext->spanId->toHex());
        static::assertTrue($ctx->spanContext->traceFlags->isSampled());
        static::assertTrue($ctx->spanContext->isRemote);
        static::assertSame('00f067aa0ba902b7', $ctx->spanContext->traceState->get('rojo'));
        static::assertSame('t61rcWkgMzE', $ctx->spanContext->traceState->get('congo'));
    }

    public function test_extract_with_composite_propagator_from_symfony_request(): void
    {
        $request = Request::create(
            '/api/orders',
            'POST',
            [],
            [],
            [],
            [
                'HTTP_TRACEPARENT' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
                'HTTP_BAGGAGE' => 'userId=alice,requestId=req-123',
            ],
        );

        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);
        $carrier = new RequestCarrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());

        static::assertNotNull($ctx->baggage);
        static::assertSame('alice', $ctx->baggage->get('userId'));
        static::assertSame('req-123', $ctx->baggage->get('requestId'));
    }

    public function test_inject_baggage_into_symfony_response(): void
    {
        $response = new Response();

        $propagator = new W3CBaggage();
        $carrier = new ResponseCarrier($response);
        $ctx = new PropagationContext(baggage: new Baggage([
            'userId' => 'alice',
            'serverNode' => 'DF28',
        ]));

        $propagator->inject($ctx, $carrier);

        $baggageHeader = $response->headers->get('baggage');
        static::assertNotNull($baggageHeader);
        static::assertStringContainsString('userId=alice', $baggageHeader);
        static::assertStringContainsString('serverNode=DF28', $baggageHeader);
    }

    public function test_inject_trace_context_into_symfony_response(): void
    {
        $response = new Response();

        $propagator = new W3CTraceContext();
        $carrier = new ResponseCarrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('rojo', 'value'),
        );
        $ctx = new PropagationContext($spanContext);

        $propagator->inject($ctx, $carrier);

        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $response->headers->get('traceparent'),
        );
        static::assertSame('rojo=value', $response->headers->get('tracestate'));
    }

    public function test_inject_with_composite_propagator_into_symfony_response(): void
    {
        $response = new Response();

        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);
        $carrier = new ResponseCarrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext, new Baggage(['userId' => 'alice']));

        $propagator->inject($ctx, $carrier);

        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $response->headers->get('traceparent'),
        );
        static::assertSame('userId=alice', $response->headers->get('baggage'));
    }

    public function test_round_trip_request_to_response_preserves_context(): void
    {
        $incomingRequest = Request::create(
            '/api/process',
            'POST',
            [],
            [],
            [],
            [
                'HTTP_TRACEPARENT' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
                'HTTP_TRACESTATE' => 'vendor=data',
                'HTTP_BAGGAGE' => 'userId=alice,sessionId=sess-456',
            ],
        );

        $propagator = new CompositePropagator([
            new W3CTraceContext(),
            new W3CBaggage(),
        ]);

        $requestCarrier = new RequestCarrier($incomingRequest);
        $extractedCtx = $propagator->extract($requestCarrier);

        static::assertNotNull($extractedCtx->spanContext);
        static::assertNotNull($extractedCtx->baggage);

        $outgoingResponse = new Response();
        $responseCarrier = new ResponseCarrier($outgoingResponse);
        $propagator->inject($extractedCtx, $responseCarrier);

        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $outgoingResponse->headers->get('traceparent'),
        );
        static::assertSame('vendor=data', $outgoingResponse->headers->get('tracestate'));

        $baggageHeader = $outgoingResponse->headers->get('baggage');
        static::assertNotNull($baggageHeader);
        static::assertStringContainsString('userId=alice', $baggageHeader);
        static::assertStringContainsString('sessionId=sess-456', $baggageHeader);
    }

    public function test_verify_response_headers_via_response_carrier_get(): void
    {
        $response = new Response();

        $propagator = new W3CTraceContext();
        $carrier = new ResponseCarrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext);

        $propagator->inject($ctx, $carrier);

        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $carrier->get('traceparent'));
    }
}
