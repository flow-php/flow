<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use DateTimeImmutable;
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
use function hrtime;

final readonly class TracingMiddleware implements MiddlewareInterface
{
    public function __construct(
        private Telemetry $telemetry,
        private ?ContextStorage $contextStorage = null,
        private ?Propagator $propagator = null,
        private bool $traceHandler = true,
        private bool $metrics = true,
        private MessengerMetricDurationUnit $durationUnit = MessengerMetricDurationUnit::Seconds,
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
        $operation = $isReceived ? 'process' : 'send';

        $busName = $busNameStamp instanceof BusNameStamp ? $busNameStamp->getBusName() : 'default';
        $spanName = "{$operation} {$shortMessageClass}";

        $attributes = [
            'messaging.system' => 'symfony_messenger',
            'messaging.destination.name' => $shortMessageClass,
            'messaging.message.class' => $messageClass,
            'messaging.operation.type' => $operation,
            'messaging.operation.name' => $operation,
            'messaging.symfony.bus' => $busName,
        ];

        if ($receivedStamp instanceof ReceivedStamp) {
            $attributes['messaging.transport'] = $receivedStamp->getTransportName();
        }

        if ($transportIdStamp instanceof TransportMessageIdStamp) {
            $attributes['messaging.message.id'] = (string) $transportIdStamp->getId();
        }

        $meter = $this->metrics
            ? $this->telemetry->meter('flow.symfony.messenger', PackageVersion::get('symfony/messenger'))
            : null;

        $metricAttributes = [
            'messaging.system' => 'symfony_messenger',
            'messaging.operation.name' => $operation,
            'messaging.destination.name' => $shortMessageClass,
        ];

        if ($receivedStamp instanceof ReceivedStamp) {
            $metricAttributes['messaging.consumer.group.name'] = $receivedStamp->getTransportName();
        }

        $processDuration = $meter !== null && $isReceived
            ? $meter->createHistogram(
                'messaging.process.duration',
                $this->durationUnit->value,
                'Duration of processing a consumed message',
                $this->durationUnit->histogramBoundaries(),
            )
            : null;

        $remote = $isReceived ? $this->extractRemoteContext($envelope) : null;
        $links = [];
        $remoteBaggage = null;

        if ($remote !== null && $remote->spanContext !== null) {
            $remoteBaggage = $remote->baggage;

            $links[] = SpanLink::create(
                SpanContext::createRemote($remote->spanContext->traceId, $remote->spanContext->spanId),
                ['messaging.operation.type' => 'process'],
            );
        }

        // Build the handling context: lift the worker poll suppression so the handler's own instrumentation
        // is recorded, and carry the producer's baggage into handling.
        $propagationScope = null;

        if ($isReceived && $this->contextStorage !== null) {
            $handlingContext = $this->contextStorage->current();
            $mutated = false;

            if ($this->traceHandler && $handlingContext->isTracingSuppressed()) {
                $handlingContext = $handlingContext->withoutSuppressedTracing();
                $mutated = true;
            }

            if ($remoteBaggage !== null) {
                $handlingContext = $handlingContext->withBaggage($remoteBaggage);
                $mutated = true;
            }

            if ($mutated) {
                $propagationScope = $this->contextStorage->attach($handlingContext);
            }
        }

        if ($meter !== null) {
            $meter->createCounter(
                $isReceived ? 'messaging.client.consumed.messages' : 'messaging.client.sent.messages',
                '{message}',
                $isReceived
                    ? 'Number of messages delivered to the application'
                    : 'Number of messages sent to the broker',
            )->add(1, $metricAttributes);
        }

        // A consumed message starts its own trace (linked to the producer), per the OTEL messaging conventions.
        $span = $this->traceHandler
            ? $tracer->span($spanName, $kind, $attributes, $links, $isReceived ? false : null)
            : null;

        if (!$isReceived) {
            $envelope = $this->injectContext($envelope);
        }

        $startedAt = $span === null ? hrtime(true) : null;
        $errorType = null;

        try {
            return $stack->next()->handle($envelope, $stack);
        } catch (Throwable $e) {
            $errorType = $e::class;

            if ($span !== null) {
                $span->recordException($e, new DateTimeImmutable());
                $span->setAttribute('error.type', $e::class);
                $span->setStatus(SpanStatus::error($e->getMessage()));
            }

            throw $e;
        } finally {
            if ($span !== null) {
                $tracer->complete($span);
            }

            if ($processDuration !== null) {
                $durationAttributes = $metricAttributes;

                if ($errorType !== null) {
                    $durationAttributes['error.type'] = $errorType;
                }

                $durationMs = $startedAt === null
                    ? $span?->duration() ?? 0.0
                    : (float) (hrtime(true) - $startedAt) / 1_000_000;

                $processDuration->record($this->durationUnit->fromMilliseconds($durationMs), $durationAttributes);
            }

            $propagationScope?->detach();
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
        $activeSpan = $context->activeSpan();

        if ($activeSpan === null) {
            return $envelope;
        }

        $propagationContext = new PropagationContext($activeSpan, $context->baggage);

        $carrier = new TelemetryStampCarrier();
        $this->propagator->inject($propagationContext, $carrier);

        return $envelope->with($carrier->unwrap());
    }
}
