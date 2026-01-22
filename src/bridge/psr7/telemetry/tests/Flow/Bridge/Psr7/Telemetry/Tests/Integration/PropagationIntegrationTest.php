<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry\Tests\Integration;

use function Flow\Bridge\Psr7\Telemetry\DSL\{psr7_request_carrier, psr7_response_carrier};
use function Flow\Telemetry\DSL\{composite_propagator, propagation_context, w3c_baggage, w3c_trace_context};
use Flow\Telemetry\Context\{Baggage, SpanId, TraceFlags, TraceId, TraceState};
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Tracer\SpanContext;
use Nyholm\Psr7\{Response, ServerRequest};
use PHPUnit\Framework\TestCase;

final class PropagationIntegrationTest extends TestCase
{
    public function test_extract_baggage_from_psr7_request() : void
    {
        $request = new ServerRequest('GET', '/api/users', [
            'baggage' => 'userId=alice,serverNode=DF28,isProduction=false',
        ]);

        $propagator = w3c_baggage();
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->baggage);
        self::assertSame('alice', $ctx->baggage->get('userId'));
        self::assertSame('DF28', $ctx->baggage->get('serverNode'));
        self::assertSame('false', $ctx->baggage->get('isProduction'));
    }

    public function test_extract_trace_context_from_psr7_request() : void
    {
        $request = new ServerRequest('GET', '/api/users', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE',
        ]);

        $propagator = w3c_trace_context();
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->spanContext);
        self::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        self::assertSame('00f067aa0ba902b7', $ctx->spanContext->spanId->toHex());
        self::assertTrue($ctx->spanContext->traceFlags->isSampled());
        self::assertTrue($ctx->spanContext->isRemote);
        self::assertSame('00f067aa0ba902b7', $ctx->spanContext->traceState->get('rojo'));
        self::assertSame('t61rcWkgMzE', $ctx->spanContext->traceState->get('congo'));
    }

    public function test_extract_with_composite_propagator_from_psr7_request() : void
    {
        $request = new ServerRequest('POST', '/api/orders', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'baggage' => 'userId=alice,requestId=req-123',
        ]);

        $propagator = composite_propagator(w3c_trace_context(), w3c_baggage());
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        self::assertNotNull($ctx->spanContext);
        self::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());

        self::assertNotNull($ctx->baggage);
        self::assertSame('alice', $ctx->baggage->get('userId'));
        self::assertSame('req-123', $ctx->baggage->get('requestId'));
    }

    public function test_inject_baggage_into_psr7_response() : void
    {
        $response = new Response();

        $propagator = w3c_baggage();
        $carrier = psr7_response_carrier($response);
        $ctx = propagation_context(baggage: \Flow\Telemetry\DSL\baggage([
            'userId' => 'alice',
            'serverNode' => 'DF28',
        ]));

        $propagator->inject($ctx, $carrier);

        $modifiedResponse = $carrier->unwrap();
        $baggageHeader = $modifiedResponse->getHeader('baggage')[0] ?? null;
        self::assertNotNull($baggageHeader);
        self::assertStringContainsString('userId=alice', $baggageHeader);
        self::assertStringContainsString('serverNode=DF28', $baggageHeader);
    }

    public function test_inject_trace_context_into_psr7_response() : void
    {
        $response = new Response();

        $propagator = w3c_trace_context();
        $carrier = psr7_response_carrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
            TraceState::empty()->with('rojo', 'value'),
        );
        $ctx = new PropagationContext($spanContext);

        $propagator->inject($ctx, $carrier);

        $modifiedResponse = $carrier->unwrap();
        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        self::assertSame('rojo=value', $modifiedResponse->getHeader('tracestate')[0]);
    }

    public function test_inject_with_composite_propagator_into_psr7_response() : void
    {
        $response = new Response();

        $propagator = composite_propagator(w3c_trace_context(), w3c_baggage());
        $carrier = psr7_response_carrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext, new Baggage(['userId' => 'alice']));

        $propagator->inject($ctx, $carrier);

        $modifiedResponse = $carrier->unwrap();
        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        self::assertSame('userId=alice', $modifiedResponse->getHeader('baggage')[0]);
    }

    public function test_round_trip_request_to_response_preserves_context() : void
    {
        $incomingRequest = new ServerRequest('POST', '/api/process', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'vendor=data',
            'baggage' => 'userId=alice,sessionId=sess-456',
        ]);

        $propagator = composite_propagator(w3c_trace_context(), w3c_baggage());

        $requestCarrier = psr7_request_carrier($incomingRequest);
        $extractedCtx = $propagator->extract($requestCarrier);

        self::assertNotNull($extractedCtx->spanContext);
        self::assertNotNull($extractedCtx->baggage);

        $outgoingResponse = new Response();
        $responseCarrier = psr7_response_carrier($outgoingResponse);
        $propagator->inject($extractedCtx, $responseCarrier);

        $modifiedResponse = $responseCarrier->unwrap();
        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        self::assertSame('vendor=data', $modifiedResponse->getHeader('tracestate')[0]);

        $baggageHeader = $modifiedResponse->getHeader('baggage')[0] ?? null;
        self::assertNotNull($baggageHeader);
        self::assertStringContainsString('userId=alice', $baggageHeader);
        self::assertStringContainsString('sessionId=sess-456', $baggageHeader);
    }

    public function test_verify_response_headers_via_response_carrier_get() : void
    {
        $response = new Response();

        $propagator = w3c_trace_context();
        $carrier = psr7_response_carrier($response);
        $spanContext = SpanContext::create(
            TraceId::fromHex('0af7651916cd43dd8448eb211c80319c'),
            SpanId::fromHex('00f067aa0ba902b7'),
            null,
            TraceFlags::sampled(),
        );
        $ctx = new PropagationContext($spanContext);

        $propagator->inject($ctx, $carrier);

        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $carrier->get('traceparent'),
        );
    }
}
