<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Propagation;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Tracer\Span;
use Symfony\Component\HttpFoundation\RequestStack;

use function http_build_query;
use function str_contains;

final readonly class TraceContextProvider
{
    public function __construct(
        private Propagator $propagator,
        private ContextStorage $contextStorage,
        private ?RequestStack $requestStack = null,
    ) {}

    /**
     * Append the current trace context to a URL's query string (no-op when there is no request span).
     */
    public function appendToUrl(string $url): string
    {
        $context = $this->current();

        if ($context === []) {
            return $url;
        }

        return $url . (str_contains($url, '?') ? '&' : '?') . http_build_query($context);
    }

    /**
     * The request span's trace context as propagation fields (e.g. traceparent, tracestate, baggage), or
     * an empty array when there is no request span.
     *
     * @return array<string, string>
     */
    public function current(): array
    {
        $request = $this->requestStack?->getMainRequest();

        if ($request === null) {
            return [];
        }

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE)) instanceof Span) {
            return [];
        }

        $carrier = new ArrayCarrier();
        $this->propagator->inject(
            new PropagationContext($span->context(), $this->contextStorage->current()->baggage),
            $carrier,
        );

        return $carrier->unwrap();
    }

    public function traceparent(): string
    {
        return $this->current()['traceparent'] ?? '';
    }
}
