<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use DateTimeImmutable;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Throwable;

use function end;
use function explode;

final readonly class TracingMiddleware implements MiddlewareInterface
{
    public function __construct(
        private Telemetry $telemetry,
        private ?ContextStorage $contextStorage = null,
        private ?Propagator $propagator = null,
        private MessengerTracePropagation $propagation = MessengerTracePropagation::Link,
    ) {}

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $tracer = $this->telemetry->tracer('flow.symfony.messenger', PackageVersion::get('symfony/messenger'));

        $message = $envelope->getMessage();
        $messageClass = $message::class;
        $shortMessageClass = $this->getShortClassName($messageClass);

        $receivedStamp = $envelope->last(ReceivedStamp::class);
        $busNameStamp = $envelope->last(BusNameStamp::class);
        $transportIdStamp = $envelope->last(TransportMessageIdStamp::class);

        $isReceived = $receivedStamp !== null;

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

        $remote = $isReceived ? $this->extractRemoteContext($envelope) : null;
        $links = [];

        if ($remote !== null && $remote->spanContext !== null) {
            $remoteSpanContext = $remote->spanContext;
            $remoteBaggage = $remote->baggage;

            if ($this->propagation === MessengerTracePropagation::Continuation) {
                $context = Context::withTraceId($remoteSpanContext->traceId)->withActiveSpan($remoteSpanContext->spanId);

                if ($remoteBaggage !== null) {
                    $context = $context->withBaggage($remoteBaggage);
                }

                $this->contextStorage?->attach($context);
            } else {
                $links[] = SpanLink::create(
                    SpanContext::createRemote($remoteSpanContext->traceId, $remoteSpanContext->spanId),
                    ['messaging.operation.type' => 'process'],
                );

                if ($remoteBaggage !== null && $this->contextStorage !== null) {
                    $this->contextStorage->attach($this->contextStorage->current()->withBaggage($remoteBaggage));
                }
            }
        }

        $span = $tracer->span($spanName, $kind, $attributes, $links);

        if (!$isReceived) {
            $envelope = $this->injectContext($envelope);
        }

        try {
            $result = $stack->next()->handle($envelope, $stack);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $e) {
            $span->recordException($e, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $tracer->complete($span);
        }
    }

    private function extractRemoteContext(Envelope $envelope): ?PropagationContext
    {
        if ($this->contextStorage === null || $this->propagator === null) {
            return null;
        }

        $stamp = $envelope->last(TelemetryStamp::class);

        if (!$stamp instanceof TelemetryStamp) {
            return null;
        }

        return $this->propagator->extract(new TelemetryStampCarrier($stamp));
    }

    private function getShortClassName(string $className): string
    {
        $parts = explode('\\', $className);

        return end($parts);
    }

    private function injectContext(Envelope $envelope): Envelope
    {
        if ($this->contextStorage === null || $this->propagator === null) {
            return $envelope;
        }

        $context = $this->contextStorage->current();
        $activeSpanId = $context->activeSpanId();

        if ($activeSpanId === null) {
            return $envelope;
        }

        $spanContext = SpanContext::create($context->traceId, $activeSpanId);

        $propagationContext = new PropagationContext($spanContext, $context->baggage);

        $carrier = new TelemetryStampCarrier();
        $this->propagator->inject($propagationContext, $carrier);

        return $envelope->with($carrier->unwrap());
    }
}
