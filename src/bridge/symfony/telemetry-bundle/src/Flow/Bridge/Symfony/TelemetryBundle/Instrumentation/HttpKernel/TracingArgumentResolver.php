<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use ReflectionFunctionAbstract;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ArgumentResolverInterface;

final readonly class TracingArgumentResolver implements ArgumentResolverInterface
{
    public function __construct(
        private ArgumentResolverInterface $resolver,
        private Telemetry $telemetry,
    ) {}

    public function getArguments(
        Request $request,
        callable $controller,
        ?ReflectionFunctionAbstract $reflector = null,
    ): array {
        if (!$request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE) instanceof Span) {
            return $this->resolver->getArguments($request, $controller, $reflector);
        }

        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span('controller.get_arguments', SpanKind::INTERNAL);

        try {
            return $this->resolver->getArguments($request, $controller, $reflector);
        } finally {
            $span->setStatus(SpanStatus::ok());
            $tracer->complete($span);
        }
    }
}
