<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Telemetry\Context\{Context, ContextStorage};
use Flow\Telemetry\{PackageVersion, Telemetry};
use Flow\Telemetry\Propagation\{PropagationContext, Propagator};
use Flow\Telemetry\Tracer\{SpanContext, SpanKind, SpanStatus};
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\{MiddlewareInterface, StackInterface};
use Symfony\Component\Messenger\Stamp\{BusNameStamp, ReceivedStamp, TransportMessageIdStamp};

final readonly class TracingMiddleware implements MiddlewareInterface
{
    public function __construct(
        private Telemetry $telemetry,
        private ?ContextStorage $contextStorage = null,
        private ?Propagator $propagator = null,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack) : Envelope
    {
        $tracer = $this->telemetry->tracer('flow.symfony.messenger', PackageVersion::get('symfony/messenger'));

        $message = $envelope->getMessage();
        $messageClass = $message::class;
        $shortMessageClass = $this->getShortClassName($messageClass);

        $receivedStamp = $envelope->last(ReceivedStamp::class);
        $busNameStamp = $envelope->last(BusNameStamp::class);
        $transportIdStamp = $envelope->last(TransportMessageIdStamp::class);

        $isReceived = $receivedStamp !== null;

        if ($isReceived) {
            $this->extractContext($envelope);
        }

        $kind = $isReceived ? SpanKind::CONSUMER : SpanKind::PRODUCER;
        $operation = $isReceived ? 'receive' : 'send';

        $busName = $busNameStamp instanceof BusNameStamp ? $busNameStamp->getBusName() : 'default';
        $spanName = "{$busName} {$shortMessageClass}";

        $attributes = [
            'messaging.system' => 'symfony_messenger',
            'messaging.destination.name' => $busName,
            'messaging.message.class' => $messageClass,
            'messaging.operation.type' => $operation,
            'messaging.operation.name' => $messageClass,
        ];

        if ($receivedStamp instanceof ReceivedStamp) {
            $attributes['messaging.transport'] = $receivedStamp->getTransportName();
        }

        if ($transportIdStamp instanceof TransportMessageIdStamp) {
            $attributes['messaging.message.id'] = (string) $transportIdStamp->getId();
        }

        $span = $tracer->span($spanName, $kind, $attributes);

        if (!$isReceived) {
            $envelope = $this->injectContext($envelope);
        }

        try {
            $result = $stack->next()->handle($envelope, $stack);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $tracer->complete($span);
        }
    }

    private function extractContext(Envelope $envelope) : void
    {
        if ($this->contextStorage === null || $this->propagator === null) {
            return;
        }

        $stamp = $envelope->last(TelemetryStamp::class);

        if (!$stamp instanceof TelemetryStamp) {
            return;
        }

        $carrier = new TelemetryStampCarrier($stamp);
        $propagationContext = $this->propagator->extract($carrier);

        if ($propagationContext->spanContext !== null) {
            $context = Context::withTraceId($propagationContext->spanContext->traceId);
            $context = $context->withActiveSpan($propagationContext->spanContext->spanId);

            if ($propagationContext->baggage !== null) {
                $context = $context->withBaggage($propagationContext->baggage);
            }

            $this->contextStorage->store($context);
        }
    }

    private function getShortClassName(string $className) : string
    {
        $parts = \explode('\\', $className);

        return \end($parts);
    }

    private function injectContext(Envelope $envelope) : Envelope
    {
        if ($this->contextStorage === null || $this->propagator === null) {
            return $envelope;
        }

        $context = $this->contextStorage->current();
        $activeSpanId = $context->activeSpanId();

        if ($activeSpanId === null) {
            return $envelope;
        }

        $spanContext = SpanContext::create(
            $context->traceId,
            $activeSpanId,
        );

        $propagationContext = new PropagationContext($spanContext, $context->baggage);

        $carrier = new TelemetryStampCarrier();
        $this->propagator->inject($propagationContext, $carrier);

        return $envelope->with($carrier->unwrap());
    }
}
