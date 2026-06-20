<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ValueResolverInterface;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;

final readonly class TracingValueResolver implements ValueResolverInterface
{
    public function __construct(
        private ValueResolverInterface $inner,
        private Telemetry $telemetry,
    ) {}

    public function resolve(Request $request, ArgumentMetadata $argument): iterable
    {
        if (!$request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE) instanceof Span) {
            yield from $this->inner->resolve($request, $argument);

            return;
        }

        $tracer = $this->telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'));
        $span = $tracer->span('controller.argument_value_resolver', SpanKind::INTERNAL, [
            'code.namespace' => $this->inner::class,
            'controller.argument' => $argument->getName(),
        ]);

        try {
            yield from $this->inner->resolve($request, $argument);
            $span->setStatus(SpanStatus::ok());
        } finally {
            $tracer->complete($span);
        }
    }
}
