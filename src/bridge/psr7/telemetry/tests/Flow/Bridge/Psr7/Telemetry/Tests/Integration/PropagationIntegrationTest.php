<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry\Tests\Integration;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Tracer\SpanContext;
use Nyholm\Psr7\Response;
use Nyholm\Psr7\ServerRequest;
use PHPUnit\Framework\TestCase;

use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_request_carrier;
use function Flow\Bridge\Psr7\Telemetry\DSL\psr7_response_carrier;
use function Flow\Telemetry\DSL\baggage;
use function Flow\Telemetry\DSL\composite_propagator;
use function Flow\Telemetry\DSL\propagation_context;
use function Flow\Telemetry\DSL\w3c_baggage;
use function Flow\Telemetry\DSL\w3c_trace_context;

final class PropagationIntegrationTest extends TestCase
{
    public function test_extract_baggage_from_psr7_request(): void
    {
        $request = new ServerRequest('GET', '/api/users', [
            'baggage' => 'userId=alice,serverNode=DF28,isProduction=false',
        ]);

        $propagator = w3c_baggage();
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->baggage);
        static::assertSame('alice', $ctx->baggage->get('userId'));
        static::assertSame('DF28', $ctx->baggage->get('serverNode'));
        static::assertSame('false', $ctx->baggage->get('isProduction'));
    }

    public function test_extract_trace_context_from_psr7_request(): void
    {
        $request = new ServerRequest('GET', '/api/users', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'rojo=00f067aa0ba902b7,congo=t61rcWkgMzE',
        ]);

        $propagator = w3c_trace_context();
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());
        static::assertSame('00f067aa0ba902b7', $ctx->spanContext->spanId->toHex());
        static::assertTrue($ctx->spanContext->traceFlags->isSampled());
        static::assertTrue($ctx->spanContext->isRemote);
        static::assertSame('00f067aa0ba902b7', $ctx->spanContext->traceState->get('rojo'));
        static::assertSame('t61rcWkgMzE', $ctx->spanContext->traceState->get('congo'));
    }

    public function test_extract_with_composite_propagator_from_psr7_request(): void
    {
        $request = new ServerRequest('POST', '/api/orders', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'baggage' => 'userId=alice,requestId=req-123',
        ]);

        $propagator = composite_propagator(w3c_trace_context(), w3c_baggage());
        $carrier = psr7_request_carrier($request);

        $ctx = $propagator->extract($carrier);

        static::assertNotNull($ctx->spanContext);
        static::assertSame('0af7651916cd43dd8448eb211c80319c', $ctx->spanContext->traceId->toHex());

        static::assertNotNull($ctx->baggage);
        static::assertSame('alice', $ctx->baggage->get('userId'));
        static::assertSame('req-123', $ctx->baggage->get('requestId'));
    }

    public function test_inject_baggage_into_psr7_response(): void
    {
        $response = new Response();

        $propagator = w3c_baggage();
        $carrier = psr7_response_carrier($response);
        $ctx = propagation_context(baggage: baggage([
            'userId' => 'alice',
            'serverNode' => 'DF28',
        ]));

        $propagator->inject($ctx, $carrier);

        $modifiedResponse = $carrier->unwrap();
        $baggageHeader = $modifiedResponse->getHeader('baggage')[0] ?? null;
        static::assertNotNull($baggageHeader);
        static::assertStringContainsString('userId=alice', $baggageHeader);
        static::assertStringContainsString('serverNode=DF28', $baggageHeader);
    }

    public function test_inject_trace_context_into_psr7_response(): void
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
        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        static::assertSame('rojo=value', $modifiedResponse->getHeader('tracestate')[0]);
    }

    public function test_inject_with_composite_propagator_into_psr7_response(): void
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
        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        static::assertSame('userId=alice', $modifiedResponse->getHeader('baggage')[0]);
    }

    public function test_round_trip_request_to_response_preserves_context(): void
    {
        $incomingRequest = new ServerRequest('POST', '/api/process', [
            'traceparent' => '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            'tracestate' => 'vendor=data',
            'baggage' => 'userId=alice,sessionId=sess-456',
        ]);

        $propagator = composite_propagator(w3c_trace_context(), w3c_baggage());

        $requestCarrier = psr7_request_carrier($incomingRequest);
        $extractedCtx = $propagator->extract($requestCarrier);

        static::assertNotNull($extractedCtx->spanContext);
        static::assertNotNull($extractedCtx->baggage);

        $outgoingResponse = new Response();
        $responseCarrier = psr7_response_carrier($outgoingResponse);
        $propagator->inject($extractedCtx, $responseCarrier);

        $modifiedResponse = $responseCarrier->unwrap();
        static::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $modifiedResponse->getHeader('traceparent')[0],
        );
        static::assertSame('vendor=data', $modifiedResponse->getHeader('tracestate')[0]);

        $baggageHeader = $modifiedResponse->getHeader('baggage')[0] ?? null;
        static::assertNotNull($baggageHeader);
        static::assertStringContainsString('userId=alice', $baggageHeader);
        static::assertStringContainsString('sessionId=sess-456', $baggageHeader);
    }

    public function test_verify_response_headers_via_response_carrier_get(): void
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

        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $carrier->get('traceparent'));
    }
}
