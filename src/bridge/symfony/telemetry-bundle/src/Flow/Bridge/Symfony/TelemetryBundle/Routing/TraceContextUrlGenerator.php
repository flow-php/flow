<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Routing;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Symfony\Component\Routing\Generator\UrlGeneratorInterface;
use Symfony\Component\Routing\RequestContext;

/**
 * Wraps the router so generated URLs carry the current trace context. This is an opt-in service
 * (inject it explicitly); it does not replace the default router, so ordinary path()/url() calls are
 * unaffected.
 */
final readonly class TraceContextUrlGenerator implements UrlGeneratorInterface
{
    public function __construct(
        private UrlGeneratorInterface $urlGenerator,
        private TraceContextProvider $traceContext,
    ) {}

    public function generate(string $name, array $parameters = [], int $referenceType = self::ABSOLUTE_PATH): string
    {
        return $this->traceContext->appendToUrl($this->urlGenerator->generate($name, $parameters, $referenceType));
    }

    public function getContext(): RequestContext
    {
        return $this->urlGenerator->getContext();
    }

    public function setContext(RequestContext $context): void
    {
        $this->urlGenerator->setContext($context);
    }
}
