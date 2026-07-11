<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Twig\Extension\AbstractExtension;
use Twig\TwigFunction;

use function htmlspecialchars;
use function implode;
use function sprintf;

use const ENT_QUOTES;

/**
 * Exposes the current trace context to templates so it can ride along to subsequent requests
 * (AJAX via headers, or links / multi-step flows via the URL query string).
 */
final class TelemetryPropagationExtension extends AbstractExtension
{
    public function __construct(
        private readonly TraceContextProvider $traceContext,
    ) {}

    public function getFunctions(): array
    {
        return [
            new TwigFunction('flow_traceparent', $this->traceContext->traceparent(...)),
            new TwigFunction('flow_trace_context', $this->traceContext->current(...)),
            new TwigFunction('flow_trace_context_meta', $this->renderMeta(...), ['is_safe' => ['html']]),
            new TwigFunction('flow_trace_context_url', $this->traceContext->appendToUrl(...)),
        ];
    }

    public function renderMeta(): string
    {
        $tags = [];

        foreach ($this->traceContext->current() as $name => $value) {
            $tags[] = sprintf(
                '<meta name="%s" content="%s">',
                htmlspecialchars($name, ENT_QUOTES),
                htmlspecialchars($value, ENT_QUOTES),
            );
        }

        return implode("\n", $tags);
    }
}
