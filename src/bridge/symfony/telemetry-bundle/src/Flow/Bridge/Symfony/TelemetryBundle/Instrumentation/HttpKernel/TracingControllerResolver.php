<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ControllerResolverInterface;
use Throwable;

final readonly class TracingControllerResolver implements ControllerResolverInterface
{
    public function __construct(
        private ControllerResolverInterface $resolver,
        private Telemetry $telemetry,
    ) {}

    public function getController(Request $request): callable|false
    {
        if (!$request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE) instanceof Span) {
            return $this->resolver->getController($request);
        }

        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span('controller.get_callable', SpanKind::INTERNAL);

        try {
            return $this->resolver->getController($request);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            // OTEL spec: instrumentation leaves the status Unset on success.
            $tracer->complete($span);
        }
    }
}
