<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Routing;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Symfony\Component\Routing\Generator\UrlGeneratorInterface;
use Symfony\Component\Routing\RequestContext;

/**
 * Opt-in: does not replace the default router.
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
