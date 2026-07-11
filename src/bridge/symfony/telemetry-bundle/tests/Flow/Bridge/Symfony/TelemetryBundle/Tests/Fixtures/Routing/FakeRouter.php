<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Routing;

use Symfony\Component\Routing\RequestContext;
use Symfony\Component\Routing\RouteCollection;
use Symfony\Component\Routing\RouterInterface;

final class FakeRouter implements RouterInterface
{
    public int $getRouteCollectionCalls = 0;

    private RequestContext $context;

    public function __construct(
        private RouteCollection $routes = new RouteCollection(),
    ) {
        $this->context = new RequestContext();
    }

    public function generate(string $name, array $parameters = [], int $referenceType = self::ABSOLUTE_PATH): string
    {
        return '/generated';
    }

    public function getContext(): RequestContext
    {
        return $this->context;
    }

    public function getRouteCollection(): RouteCollection
    {
        $this->getRouteCollectionCalls++;

        return $this->routes;
    }

    public function match(string $pathinfo): array
    {
        return [];
    }

    public function setContext(RequestContext $context): void
    {
        $this->context = $context;
    }

    public function setRouteCollection(RouteCollection $routes): void
    {
        $this->routes = $routes;
    }
}
