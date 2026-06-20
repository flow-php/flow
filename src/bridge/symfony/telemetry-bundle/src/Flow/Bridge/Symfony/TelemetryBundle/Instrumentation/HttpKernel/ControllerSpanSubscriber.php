<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\ControllerArgumentsEvent;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\Event\ViewEvent;
use Symfony\Component\HttpKernel\KernelEvents;

use function is_string;

final readonly class ControllerSpanSubscriber implements EventSubscriberInterface
{
    private const string CONTROLLER_SPAN_ATTRIBUTE = '_flow_telemetry_controller_span';

    public function __construct(
        private Telemetry $telemetry,
        private bool $traceController = true,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::CONTROLLER_ARGUMENTS => ['onControllerArguments', -10000],
            KernelEvents::VIEW => ['onComplete', 10000],
            KernelEvents::RESPONSE => ['onComplete', 10000],
            KernelEvents::EXCEPTION => ['onException', 10000],
        ];
    }

    public function onControllerArguments(ControllerArgumentsEvent $event): void
    {
        if (!$this->traceController) {
            return;
        }

        $request = $event->getRequest();

        if (!$request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE) instanceof Span) {
            return;
        }

        if ($request->attributes->get(self::CONTROLLER_SPAN_ATTRIBUTE) instanceof Span) {
            return;
        }

        $resolved = ControllerName::resolve($event->getController());
        $name = 'controller';

        /** @var array<string, scalar> $attributes */
        $attributes = [];

        if ($resolved !== null) {
            $name = $resolved->name;
            $attributes['controller'] = $resolved->name;

            if ($resolved->namespace !== null) {
                $attributes['code.namespace'] = $resolved->namespace;
            }

            if ($resolved->function !== null) {
                $attributes['code.function'] = $resolved->function;
            }
        }

        // @mago-expect analysis:mixed-assignment
        if (is_string($route = $request->attributes->get('_route'))) {
            $attributes['http.route'] = $route;
        }

        $span = $this->tracer()->span($name, SpanKind::INTERNAL, $attributes);

        $request->attributes->set(self::CONTROLLER_SPAN_ATTRIBUTE, $span);
    }

    public function onComplete(ViewEvent|ResponseEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::CONTROLLER_SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        $span->setStatus(SpanStatus::ok());
        $this->tracer()->complete($span);

        $request->attributes->remove(self::CONTROLLER_SPAN_ATTRIBUTE);
    }

    public function onException(ExceptionEvent $event): void
    {
        $request = $event->getRequest();

        // @mago-expect analysis:mixed-assignment
        if (!($span = $request->attributes->get(self::CONTROLLER_SPAN_ATTRIBUTE)) instanceof Span) {
            return;
        }

        $throwable = $event->getThrowable();
        $span->recordException($throwable, new DateTimeImmutable());
        $span->setStatus(SpanStatus::error($throwable->getMessage()));
        $this->tracer()->complete($span);

        $request->attributes->remove(self::CONTROLLER_SPAN_ATTRIBUTE);
    }

    private function tracer(): Tracer
    {
        return $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
    }
}
