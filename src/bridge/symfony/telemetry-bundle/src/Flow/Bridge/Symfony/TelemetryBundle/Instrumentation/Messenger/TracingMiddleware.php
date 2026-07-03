<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use DateTimeImmutable;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Propagation\PropagationContext;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\SemConvMetrics;
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

use function hrtime;

final readonly class TracingMiddleware implements MiddlewareInterface
{
    /**
     * messaging.process.duration is spec-fixed at seconds; boundaries are the semconv-advised set.
     *
     * @see https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
     */
    private const array PROCESS_DURATION_BOUNDARIES = [
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
    ];

    public function __construct(
        private Telemetry $telemetry,
        private ?ContextStorage $contextStorage = null,
        private ?Propagator $propagator = null,
        private bool $traceHandler = true,
        private bool $metrics = true,
    ) {}

    public function handle(Envelope $envelope, StackInterface $stack): Envelope
    {
        $tracer = $this->telemetry->tracer('flow.symfony.messenger', PackageVersion::get('symfony/messenger'));

        $message = $envelope->getMessage();
        $messageClass = $message::class;

        $receivedStamp = $envelope->last(ReceivedStamp::class);
        $busNameStamp = $envelope->last(BusNameStamp::class);
        $transportIdStamp = $envelope->last(TransportMessageIdStamp::class);

        $isReceived = $receivedStamp !== null;

        $kind = $isReceived ? SpanKind::CONSUMER : SpanKind::PRODUCER;
        $operation = $isReceived ? 'process' : 'send';

        $busName = $busNameStamp instanceof BusNameStamp ? $busNameStamp->getBusName() : 'default';

        // OTEL messaging semconv: messaging.destination.name is the queue/topic - the transport in
        // Symfony terms. It is only known on the consume side; on dispatch the routing to a
        // transport has not happened yet, so the producer span is named after the operation alone.
        $transportName = $receivedStamp instanceof ReceivedStamp ? $receivedStamp->getTransportName() : null;
        $spanName = $transportName !== null ? "{$operation} {$transportName}" : $operation;

        $attributes = [
            SemConvAttributes::MESSAGING_SYSTEM => 'symfony_messenger',
            MessengerAttributes::ATTR_MESSAGE_CLASS => $messageClass,
            SemConvAttributes::MESSAGING_OPERATION_TYPE => $operation,
            SemConvAttributes::MESSAGING_OPERATION_NAME => $operation,
            MessengerAttributes::ATTR_BUS => $busName,
        ];

        if ($transportName !== null) {
            $attributes[SemConvAttributes::MESSAGING_DESTINATION_NAME] = $transportName;
        }

        if ($transportIdStamp instanceof TransportMessageIdStamp) {
            $attributes[SemConvAttributes::MESSAGING_MESSAGE_ID] = (string) $transportIdStamp->getId();
        }

        $meter = $this->metrics
            ? $this->telemetry->meter('flow.symfony.messenger', PackageVersion::get('symfony/messenger'))
            : null;

        $metricAttributes = [
            SemConvAttributes::MESSAGING_SYSTEM => 'symfony_messenger',
            SemConvAttributes::MESSAGING_OPERATION_NAME => $operation,
        ];

        if ($transportName !== null) {
            $metricAttributes[SemConvAttributes::MESSAGING_DESTINATION_NAME] = $transportName;
        }

        $processDuration = $meter !== null && $isReceived
            ? $meter->createHistogram(
                SemConvMetrics::MESSAGING_PROCESS_DURATION,
                's',
                'Duration of processing a consumed message',
                self::PROCESS_DURATION_BOUNDARIES,
            )
            : null;

        $remote = $isReceived ? $this->extractRemoteContext($envelope) : null;
        $links = [];
        $remoteBaggage = null;

        if ($remote !== null && $remote->spanContext !== null) {
            $remoteBaggage = $remote->baggage;

            $links[] = SpanLink::create(
                SpanContext::createRemote($remote->spanContext->traceId, $remote->spanContext->spanId),
                [SemConvAttributes::MESSAGING_OPERATION_TYPE => 'process'],
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
                $isReceived
                    ? SemConvMetrics::MESSAGING_CLIENT_CONSUMED_MESSAGES
                    : SemConvMetrics::MESSAGING_CLIENT_SENT_MESSAGES,
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
                $span->setAttribute(SemConvAttributes::ERROR_TYPE, $e::class);
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
                    $durationAttributes[SemConvAttributes::ERROR_TYPE] = $errorType;
                }

                $durationMs = $startedAt === null
                    ? $span?->duration() ?? 0.0
                    : (float) (hrtime(true) - $startedAt) / 1_000_000;

                $processDuration->record($durationMs / 1_000, $durationAttributes);
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
