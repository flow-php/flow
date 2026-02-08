<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Flow\Telemetry\{PackageVersion, Telemetry};
use Flow\Telemetry\Tracer\{Span, SpanKind, SpanStatus, Tracer};
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\{ControllerEvent, ExceptionEvent, RequestEvent, ResponseEvent, TerminateEvent};
use Symfony\Component\HttpKernel\KernelEvents;

final readonly class HttpKernelSpanSubscriber implements EventSubscriberInterface
{
    private const string SPAN_ATTRIBUTE = '_flow_telemetry_span';

    private const string TRACER_ATTRIBUTE = '_flow_telemetry_tracer';

    /**
     * @param array<string> $excludeRoutes
     */
    public function __construct(
        private Telemetry $telemetry,
        private array $excludeRoutes = [],
    ) {
    }

    public static function getSubscribedEvents() : array
    {
        return [
            KernelEvents::REQUEST => ['onRequest', 10000],
            KernelEvents::CONTROLLER => ['onController', 0],
            KernelEvents::RESPONSE => ['onResponse', -10000],
            KernelEvents::EXCEPTION => ['onException', 0],
            KernelEvents::TERMINATE => ['onTerminate', -10000],
        ];
    }

    public function onController(ControllerEvent $event) : void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
            return;
        }

        $route = $request->attributes->get('_route');

        if (\is_string($route)) {
            if (!$this->shouldTrace($route)) {
                $request->attributes->remove(self::SPAN_ATTRIBUTE);
                $request->attributes->remove(self::TRACER_ATTRIBUTE);

                return;
            }

            $span->setAttribute('http.route', $route);
            $method = $request->getMethod();
            $span->rename("{$method} {$route}");
        }

        $controller = $event->getController();
        $controllerName = $this->resolveControllerName($controller);

        if ($controllerName !== null) {
            $span->setAttribute('controller', $controllerName);
        }
    }

    public function onException(ExceptionEvent $event) : void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
            return;
        }

        $span->recordException($event->getThrowable(), new \DateTimeImmutable());
    }

    public function onRequest(RequestEvent $event) : void
    {
        $request = $event->getRequest();

        $kind = $event->isMainRequest() ? SpanKind::SERVER : SpanKind::INTERNAL;
        $method = $request->getMethod();
        $path = $request->getPathInfo();

        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span(
            "{$method} {$path}",
            $kind,
            [
                'http.method' => $method,
                'http.url' => $request->getUri(),
                'http.target' => $request->getRequestUri(),
                'http.scheme' => $request->getScheme(),
                'http.host' => $request->getHost(),
            ],
        );

        $request->attributes->set(self::SPAN_ATTRIBUTE, $span);
        $request->attributes->set(self::TRACER_ATTRIBUTE, $tracer);
    }

    public function onResponse(ResponseEvent $event) : void
    {
        $request = $event->getRequest();
        $span = $request->attributes->get(self::SPAN_ATTRIBUTE);

        if (!$span instanceof Span) {
            return;
        }

        $response = $event->getResponse();
        $statusCode = $response->getStatusCode();

        $span->setAttribute('http.status_code', $statusCode);

        if ($statusCode >= 400) {
            $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
        } else {
            $span->setStatus(SpanStatus::ok());
        }
    }

    public function onTerminate(TerminateEvent $event) : void
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

    private function matchesPattern(string $route, string $pattern) : bool
    {
        if (\str_starts_with($pattern, '/') && \str_ends_with($pattern, '/')) {
            return (bool) \preg_match($pattern, $route);
        }

        return $route === $pattern;
    }

    /**
     * @param array<int, object|string>|callable|object $controller
     */
    private function resolveControllerName(callable|object|array $controller) : ?string
    {
        if (\is_array($controller) && \count($controller) === 2) {
            $firstElement = $controller[0];
            $secondElement = $controller[1];
            $class = \is_object($firstElement) ? $firstElement::class : (\is_string($firstElement) ? $firstElement : '');
            $method = \is_string($secondElement) ? $secondElement : '';

            return "{$class}::{$method}";
        }

        if (\is_object($controller)) {
            if ($controller instanceof \Closure) {
                return 'Closure';
            }

            return $controller::class . '::__invoke';
        }

        if (\is_string($controller)) {
            return $controller;
        }

        return null;
    }

    private function shouldTrace(string $route) : bool
    {
        foreach ($this->excludeRoutes as $pattern) {
            if ($this->matchesPattern($route, $pattern)) {
                return false;
            }
        }

        return true;
    }
}
