<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Closure;
use DateTimeImmutable;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Propagation\ArrayCarrier;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ControllerEvent;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\KernelEvents;

use function array_map;
use function count;
use function is_array;
use function is_object;
use function is_string;

final readonly class HttpKernelSpanSubscriber implements EventSubscriberInterface
{
    private const string SPAN_ATTRIBUTE = '_flow_telemetry_span';

    private const string TRACER_ATTRIBUTE = '_flow_telemetry_tracer';

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
        private bool $extractContext = true,
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
            KernelEvents::TERMINATE => ['onTerminate', -10000],
        ];
    }

    public function onController(ControllerEvent $event): void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
            return;
        }

        $route = $request->attributes->get('_route');

        if (is_string($route)) {
            $span->setAttribute('http.route', $route);
        }

        $controller = $event->getController();
        $controllerName = $this->resolveControllerName($controller);

        if ($controllerName !== null) {
            $span->setAttribute('controller', $controllerName);
        }
    }

    public function onException(ExceptionEvent $event): void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
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

        if ($event->isMainRequest() && $this->extractContext) {
            $this->extractContextFromRequest($request);
        }

        $kind = $event->isMainRequest() ? SpanKind::SERVER : SpanKind::INTERNAL;

        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span("{$method} {$path}", $kind, [
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
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
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
    }

    public function onTerminate(TerminateEvent $event): void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);
        $tracer = $request->attributes->get(self::TRACER_ATTRIBUTE);

        if (!$span instanceof Span || !$tracer instanceof Tracer) {
            return;
        }

        $tracer->complete($span);

        $request->attributes->remove(self::SPAN_ATTRIBUTE);
        $request->attributes->remove(self::TRACER_ATTRIBUTE);
    }

    private function extractContextFromRequest(Request $request): void
    {
        $headers = [];

        foreach ($request->headers->all() as $key => $values) {
            if (is_array($values) && count($values) > 0 && is_string($values[0])) {
                $headers[$key] = $values[0];
            }
        }

        $carrier = new ArrayCarrier($headers);
        $propagationContext = $this->propagator->extract($carrier);

        if ($propagationContext->spanContext !== null) {
            $context = Context::withTraceId($propagationContext->spanContext->traceId);
            $context = $context->withActiveSpan($propagationContext->spanContext->spanId);

            if ($propagationContext->baggage !== null) {
                $context = $context->withBaggage($propagationContext->baggage);
            }

            $this->contextStorage->attach($context);
        }
    }

    /**
     * @param array<int, object|string>|callable|object $controller
     */
    private function resolveControllerName(callable|object|array $controller): ?string
    {
        if (is_array($controller) && count($controller) === 2) {
            $firstElement = $controller[0];
            $secondElement = $controller[1];
            $class = is_object($firstElement) ? $firstElement::class : (is_string($firstElement) ? $firstElement : '');
            $method = is_string($secondElement) ? $secondElement : '';

            return "{$class}::{$method}";
        }

        if (is_object($controller)) {
            if ($controller instanceof Closure) {
                return 'Closure';
            }

            return $controller::class . '::__invoke';
        }

        if (is_string($controller)) {
            return $controller;
        }

        return null;
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
