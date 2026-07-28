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
use Flow\Telemetry\SemConvAttributes;
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

use function array_key_exists;
use function array_map;
use function is_string;

final readonly class HttpKernelSpanSubscriber implements EventSubscriberInterface
{
    public const string SPAN_ATTRIBUTE = '_flow_telemetry_span';

    private const string TRACER_ATTRIBUTE = '_flow_telemetry_tracer';

    private const string PROPAGATION_SCOPE_ATTRIBUTE = '_flow_telemetry_propagation_scope';

    private const string REQUEST_SCOPE_ATTRIBUTE = '_flow_telemetry_request_scope';

    private const string SUPPRESSION_SCOPE_ATTRIBUTE = '_flow_telemetry_suppression_scope';

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
        private ?RouteNamePathMap $routePaths = null,
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

        $controllerName = ControllerName::resolve($event->getController())?->name;

        if ($controllerName !== null) {
            $span->setAttribute(HttpKernelAttributes::ATTR_CONTROLLER, $controllerName);
        }
    }

    private function routeValue(string $routeName): string
    {
        if ($this->routeNaming !== RouteNaming::Path) {
            return $routeName;
        }

        return $this->routePaths?->pathFor($routeName) ?? $routeName;
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
            // Excluding a path must not merely skip its own span: lower-level auto-instrumentation (DBAL,
            // cache) and app kernel.terminate listeners still run for this request and would otherwise emit
            // orphan root spans. Suppress tracing for the whole request instead, so the SuppressingSampler
            // drops every span created until the suppression scope is detached on finish_request/terminate.
            $request->attributes->set(
                self::SUPPRESSION_SCOPE_ATTRIBUTE,
                $this->contextStorage->attach($this->contextStorage->current()->withSuppressedTracing()),
            );

            return;
        }

        if ($event->isMainRequest() && $this->contextPropagation) {
            $this->extractContextFromRequest($request);
        }

        $kind = $event->isMainRequest() ? SpanKind::SERVER : SpanKind::INTERNAL;

        // OTEL HTTP semconv: the span name must be low-cardinality, so start with just the method and
        // upgrade to "{method} {route}" once the route is known (see finalizeSpanName). The raw path stays
        // on the url.path attribute.
        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $attributes = [
            SemConvAttributes::HTTP_REQUEST_METHOD => $method,
            // OTEL HTTP semconv: url.path must not carry the query string; url.query is separate
            // and url.full is a client-span attribute, so it has no place on a server span.
            SemConvAttributes::URL_PATH => $request->getPathInfo(),
            SemConvAttributes::URL_SCHEME => $request->getScheme(),
            SemConvAttributes::SERVER_ADDRESS => $request->getHost(),
        ];

        $queryString = $request->server->getString('QUERY_STRING');

        if ($queryString !== '') {
            $attributes[SemConvAttributes::URL_QUERY] = $queryString;
        }

        $userAgent = $request->headers->get('User-Agent');

        if ($userAgent !== null && $userAgent !== '') {
            $attributes[SemConvAttributes::USER_AGENT_ORIGINAL] = $userAgent;
        }

        $span = $tracer->span($method, $kind, $attributes);

        $request->attributes->set(self::SPAN_ATTRIBUTE, $span);
        $request->attributes->set(self::TRACER_ATTRIBUTE, $tracer);
        // activated: the request span is the root logical scope - controller, DBAL and cache spans nest under
        // it. Attached after the propagation scope, so it must be detached before it.
        $request->attributes->set(self::REQUEST_SCOPE_ATTRIBUTE, $tracer->activate($span));
    }

    public function onResponse(ResponseEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        $this->finalizeSpanName($span, $request);

        $response = $event->getResponse();
        $statusCode = $response->getStatusCode();

        $span->setAttribute(SemConvAttributes::HTTP_RESPONSE_STATUS_CODE, $statusCode);

        // OTEL HTTP semconv: for SpanKind.SERVER the span status MUST be left unset for 1xx-4xx; only
        // 5xx (or other server-caused failures) is an Error. A 4xx is the client's fault, not the server's.
        if ($statusCode >= 500) {
            $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, (string) $statusCode);
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
            // The main request's suppression scope (excluded path) is detached on terminate, below, so it
            // still covers kernel.terminate listeners; sub-requests never terminate, so they detach here.
            return;
        }

        $this->completeSpan($event->getRequest());
        $this->detachSuppressionScope($event->getRequest());
    }

    public function onTerminate(TerminateEvent $event): void
    {
        $this->completeSpan($event->getRequest());
        $this->detachSuppressionScope($event->getRequest());
    }

    private function detachSuppressionScope(Request $request): void
    {
        // @mago-expect analysis:mixed-assignment
        if (($scope = $request->attributes->get(self::SUPPRESSION_SCOPE_ATTRIBUTE)) instanceof Scope) {
            $scope->detach();
            $request->attributes->remove(self::SUPPRESSION_SCOPE_ATTRIBUTE);
        }
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

        // @mago-expect analysis:mixed-assignment
        if (($requestScope = $request->attributes->get(self::REQUEST_SCOPE_ATTRIBUTE)) instanceof Scope) {
            $requestScope->detach();
            $request->attributes->remove(self::REQUEST_SCOPE_ATTRIBUTE);
        }

        try {
            $tracer->complete($span);
        } finally {
            // Always detach so a completion failure cannot strand the context scope into the next
            // request when the kernel is reused (worker mode).
            // @mago-expect analysis:mixed-assignment
            if (($scope = $request->attributes->get(self::PROPAGATION_SCOPE_ATTRIBUTE)) instanceof Scope) {
                $scope->detach();
                $request->attributes->remove(self::PROPAGATION_SCOPE_ATTRIBUTE);
            }

            $request->attributes->remove(self::SPAN_ATTRIBUTE);
            $request->attributes->remove(self::TRACER_ATTRIBUTE);
        }
    }

    private function finalizeSpanName(Span $span, Request $request): void
    {
        // @mago-expect analysis:mixed-assignment
        $route = $request->attributes->get('_route');

        if (is_string($route) && $route !== '') {
            $routeValue = $this->routeValue($route);
            $span->setAttribute(SemConvAttributes::HTTP_ROUTE, $routeValue);
            $span->rename("{$request->getMethod()} {$routeValue}");

            return;
        }

        // Sub-requests (render(controller(...))) carry no route, so name them after the controller.
        $attributes = $span->attributes();

        if (
            array_key_exists(HttpKernelAttributes::ATTR_CONTROLLER, $attributes)
            && is_string($attributes[HttpKernelAttributes::ATTR_CONTROLLER])
        ) {
            $span->rename("{$request->getMethod()} {$attributes[HttpKernelAttributes::ATTR_CONTROLLER]}");
        }
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
