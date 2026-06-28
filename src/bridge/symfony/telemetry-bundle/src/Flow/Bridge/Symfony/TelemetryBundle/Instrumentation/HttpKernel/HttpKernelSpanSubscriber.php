<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use DateTimeImmutable;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\QueryCarrier;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\RequestCarrier;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\ResponseCarrier;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Event\ControllerEvent;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Event\FinishRequestEvent;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\KernelEvents;
use Symfony\Component\Routing\RouterInterface;

use function array_map;
use function is_string;

final readonly class HttpKernelSpanSubscriber implements EventSubscriberInterface
{
    public const string SPAN_ATTRIBUTE = '_flow_telemetry_span';

    private const string TRACER_ATTRIBUTE = '_flow_telemetry_tracer';

    private const string PROPAGATION_SCOPE_ATTRIBUTE = '_flow_telemetry_propagation_scope';

    /** @var array<PathExclusionRule> */
    private array $excludePathRules;

    /**
     * @param array<array{path: string, method?: null|string}> $excludePaths
     */
    public function __construct(
        private Telemetry $telemetry,
        array $excludePaths,
        private ContextStorage $contextStorage,
        private Propagator $propagator,
        private bool $contextPropagation = true,
        private bool $contextPropagationQuery = false,
        private ?RouterInterface $router = null,
        private RouteNaming $routeNaming = RouteNaming::Path,
    ) {
        $this->excludePathRules = array_map(
            static fn(array $config): PathExclusionRule => PathExclusionRule::fromConfig($config),
            $excludePaths,
        );
    }

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::REQUEST => ['onRequest', 10000],
            KernelEvents::CONTROLLER => ['onController', 0],
            KernelEvents::RESPONSE => ['onResponse', -10000],
            KernelEvents::EXCEPTION => ['onException', 0],
            KernelEvents::FINISH_REQUEST => ['onFinishRequest', -10000],
            KernelEvents::TERMINATE => ['onTerminate', -10000],
        ];
    }

    public function onController(ControllerEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        // @mago-expect analysis:mixed-assignment
        $route = $request->attributes->get('_route');
        $controllerName = ControllerName::resolve($event->getController())?->name;

        if ($controllerName !== null) {
            $span->setAttribute('controller', $controllerName);
        }

        if (is_string($route) && $route !== '') {
            $routeValue = $this->routeValue($route);
            $span->setAttribute('http.route', $routeValue);
            $span->rename("{$request->getMethod()} {$routeValue}");
        } elseif ($controllerName !== null) {
            // Sub-requests (render(controller(...))) carry no route, so name them after the controller.
            $span->rename("{$request->getMethod()} {$controllerName}");
        }
    }

    private function routeValue(string $routeName): string
    {
        if ($this->routeNaming !== RouteNaming::Path || $this->router === null) {
            return $routeName;
        }

        return $this->router->getRouteCollection()->get($routeName)?->getPath() ?? $routeName;
    }

    public function onException(ExceptionEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        $span->recordException($event->getThrowable(), new DateTimeImmutable());
    }

    public function onRequest(RequestEvent $event): void
    {
        $request = $event->getRequest();
        $method = $request->getMethod();
        $path = $request->getPathInfo();

        if (!$this->shouldTraceByPath($path, $method)) {
            return;
        }

        if ($event->isMainRequest() && $this->contextPropagation) {
            $this->extractContextFromRequest($request);
        }

        $kind = $event->isMainRequest() ? SpanKind::SERVER : SpanKind::INTERNAL;

        // OTEL HTTP semconv: the span name must be low-cardinality, so start with just the method and
        // upgrade to "{method} {route}" once the route is resolved (see onController). The raw path stays
        // on the url.path attribute.
        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span($method, $kind, [
            'http.request.method' => $method,
            'url.full' => $request->getUri(),
            'url.path' => $request->getRequestUri(),
            'url.scheme' => $request->getScheme(),
            'server.address' => $request->getHost(),
        ]);

        $request->attributes->set(self::SPAN_ATTRIBUTE, $span);
        $request->attributes->set(self::TRACER_ATTRIBUTE, $tracer);
    }

    public function onResponse(ResponseEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        $response = $event->getResponse();
        $statusCode = $response->getStatusCode();

        $span->setAttribute('http.response.status_code', $statusCode);

        if ($statusCode >= 400) {
            $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
        } else {
            $span->setStatus(SpanStatus::ok());
        }

        if ($event->isMainRequest() && $this->contextPropagation) {
            $this->injectContextIntoResponse($span, $response);
        }
    }

    /**
     * Sub-requests never reach kernel.terminate (it fires only for the main request), so their span is
     * completed here, where kernel.finish_request fires once for every request, main and sub alike.
     */
    public function onFinishRequest(FinishRequestEvent $event): void
    {
        if ($event->isMainRequest()) {
            return;
        }

        $this->completeSpan($event->getRequest());
    }

    public function onTerminate(TerminateEvent $event): void
    {
        $this->completeSpan($event->getRequest());
    }

    private function completeSpan(Request $request): void
    {
        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        // @mago-expect analysis:mixed-assignment
        if (!($tracer = $request->attributes->get(self::TRACER_ATTRIBUTE)) instanceof Tracer) {
            return;
        }

        $tracer->complete($span);

        // @mago-expect analysis:mixed-assignment
        if (($scope = $request->attributes->get(self::PROPAGATION_SCOPE_ATTRIBUTE)) instanceof Scope) {
            $scope->detach();
            $request->attributes->remove(self::PROPAGATION_SCOPE_ATTRIBUTE);
        }

        $request->attributes->remove(self::SPAN_ATTRIBUTE);
        $request->attributes->remove(self::TRACER_ATTRIBUTE);
    }

    private function extractContextFromRequest(Request $request): void
    {
        $propagationContext = $this->propagator->extract(new RequestCarrier($request));

        // Links and full-page navigations cannot send headers, so optionally fall back to the query
        // string. Headers win when both are present.
        if ($propagationContext->spanContext === null && $this->contextPropagationQuery) {
            $propagationContext = $this->propagator->extract(new QueryCarrier($request));
        }

        $spanContext = $propagationContext->spanContext;

        if ($spanContext !== null) {
            $context = (new Context())->withActiveSpan($spanContext);

            if ($propagationContext->baggage !== null) {
                $context = $context->withBaggage($propagationContext->baggage);
            }

            $request->attributes->set(self::PROPAGATION_SCOPE_ATTRIBUTE, $this->contextStorage->attach($context));
        }
    }

    private function injectContextIntoResponse(Span $span, Response $response): void
    {
        $propagationContext = new PropagationContext($span->context(), $this->contextStorage->current()->baggage);

        $this->propagator->inject($propagationContext, new ResponseCarrier($response));
    }

    private function shouldTraceByPath(string $path, string $method): bool
    {
        foreach ($this->excludePathRules as $rule) {
            if ($rule->matches($path, $method)) {
                return false;
            }
        }

        return true;
    }
}
