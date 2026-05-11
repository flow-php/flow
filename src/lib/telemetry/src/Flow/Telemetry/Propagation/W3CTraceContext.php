<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Context\TraceState;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * W3C Trace Context propagator for distributed tracing.
 *
 * Implements the W3C Trace Context specification for propagating
 * trace context across service boundaries using HTTP headers.
 *
 * Headers used:
 * - traceparent: {version}-{trace-id}-{parent-id}-{trace-flags}
 * - tracestate: Vendor-specific key-value pairs
 *
 * Example usage:
 * ```php
 * $propagator = new W3CTraceContext();
 *
 * // Extract from incoming request
 * $carrier = new ArrayCarrier($headers);
 * $ctx = $propagator->extract($carrier);
 * $spanContext = $ctx->spanContext;
 *
 * // Inject into outgoing request
 * $carrier = new ArrayCarrier();
 * $propagator->inject(new PropagationContext($spanContext), $carrier);
 * $headers = $carrier->unwrap();
 * ```
 *
 * @see https://www.w3.org/TR/trace-context/
 */
final readonly class W3CTraceContext implements Propagator
{
    public const string HEADER_TRACEPARENT = 'traceparent';

    public const string HEADER_TRACESTATE = 'tracestate';

    private const string VERSION = '00';

    /**
     * @param Carrier<mixed> $carrier
     */
    public function extract(Carrier $carrier): PropagationContext
    {
        $traceparent = $carrier->get(self::HEADER_TRACEPARENT);

        if ($traceparent === null) {
            return new PropagationContext();
        }

        $parts = \explode('-', $traceparent);

        if (\count($parts) !== 4) {
            return new PropagationContext();
        }

        [$version, $traceIdHex, $spanIdHex, $flagsHex] = $parts;

        if ($version !== self::VERSION) {
            return new PropagationContext();
        }

        if (\strlen($traceIdHex) !== 32 || !\ctype_xdigit($traceIdHex)) {
            return new PropagationContext();
        }

        if (\strlen($spanIdHex) !== 16 || !\ctype_xdigit($spanIdHex)) {
            return new PropagationContext();
        }

        if (\strlen($flagsHex) !== 2 || !\ctype_xdigit($flagsHex)) {
            return new PropagationContext();
        }

        try {
            $traceId = TraceId::fromHex($traceIdHex);
            $spanId = SpanId::fromHex($spanIdHex);
            $traceFlags = TraceFlags::fromHex($flagsHex);
        } catch (\InvalidArgumentException) {
            return new PropagationContext();
        }

        if (!$traceId->isValid() || !$spanId->isValid()) {
            return new PropagationContext();
        }

        $tracestateHeader = $carrier->get(self::HEADER_TRACESTATE);
        $traceState = TraceState::empty();

        if ($tracestateHeader !== null) {
            try {
                $traceState = TraceState::fromString($tracestateHeader);
            } catch (\InvalidArgumentException) {
            }
        }

        return new PropagationContext(SpanContext::createRemote($traceId, $spanId, null, $traceFlags, $traceState));
    }

    /**
     * @return array<string>
     */
    public function fields(): array
    {
        return [self::HEADER_TRACEPARENT, self::HEADER_TRACESTATE];
    }

    /**
     * @param Carrier<mixed> $carrier
     */
    public function inject(PropagationContext $context, Carrier $carrier): void
    {
        if ($context->spanContext === null || !$context->spanContext->isValid()) {
            return;
        }

        $traceparent = \sprintf(
            '%s-%s-%s-%s',
            self::VERSION,
            $context->spanContext->traceId->toHex(),
            $context->spanContext->spanId->toHex(),
            $context->spanContext->traceFlags->toHex(),
        );

        $carrier->set(self::HEADER_TRACEPARENT, $traceparent);

        $tracestateStr = $context->spanContext->traceState->toString();

        if ($tracestateStr !== '') {
            $carrier->set(self::HEADER_TRACESTATE, $tracestateStr);
        }
    }
}
