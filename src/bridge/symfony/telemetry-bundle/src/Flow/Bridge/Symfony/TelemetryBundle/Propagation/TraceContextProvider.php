<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Propagation;

use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;

use function http_build_query;
use function str_contains;

/**
 * Exposes the current trace context for outgoing propagation, so it can be carried to subsequent
 * requests (AJAX via headers, or links / multi-step flows via the URL query string).
 *
 * Inject this in controllers/services; the Twig helpers and {@see TraceContextUrlGenerator} delegate
 * to it.
 */
final readonly class TraceContextProvider
{
    public function __construct(
        private Propagator $propagator,
        private ContextStorage $contextStorage,
    ) {}

    /**
     * Append the current trace context to a URL's query string (no-op when there is no active span).
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
     * The current trace context as propagation fields (e.g. traceparent, tracestate, baggage), or an
     * empty array when there is no active span.
     *
     * @return array<string, string>
     */
    public function current(): array
    {
        $context = $this->contextStorage->current();
        $activeSpan = $context->activeSpan();

        if ($activeSpan === null) {
            return [];
        }

        $carrier = new ArrayCarrier();
        $this->propagator->inject(new PropagationContext($activeSpan, $context->baggage), $carrier);

        return $carrier->unwrap();
    }

    public function traceparent(): string
    {
        return $this->current()['traceparent'] ?? '';
    }
}
