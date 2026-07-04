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
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\WrappedExceptionsInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\ConsumedByWorkerStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Stamp\SentStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Throwable;

use function array_values;
use function count;
use function hrtime;
use function strrpos;
use function substr;

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
        private MessageNaming $messageNaming = MessageNaming::Transport,
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

        // SyncTransport adds ReceivedStamp but only the Worker adds ConsumedByWorkerStamp: a received
        // message without it is handled synchronously inside the current request/command.
        $isWorkerConsumed = $isReceived && $envelope->last(ConsumedByWorkerStamp::class) !== null;

        $kind = $isReceived ? SpanKind::CONSUMER : SpanKind::PRODUCER;
        $operation = $isReceived ? 'process' : 'send';

        $busName = $busNameStamp instanceof BusNameStamp ? $busNameStamp->getBusName() : 'default';

        // OTEL messaging semconv: messaging.destination.name is the queue/topic - the transport in
        // Symfony terms. It is only known on the consume side; on dispatch the routing to a
        // transport has not happened yet.
        $transportName = $receivedStamp instanceof ReceivedStamp ? $receivedStamp->getTransportName() : null;

        $spanName = match ($this->messageNaming) {
            MessageNaming::MessageName => "{$operation} " . self::shortClassName($messageClass),
            MessageNaming::MessageFqcn => "{$operation} {$messageClass}",
            MessageNaming::Transport => $transportName !== null ? "{$operation} {$transportName}" : $operation,
        };

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

        // A synchronously received message keeps the live context — its producer span is already the
        // active span, so the stamp context would only duplicate the parent as a link.
        $remote = $isWorkerConsumed ? $this->extractRemoteContext($envelope) : null;
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

        // A worker-consumed message starts its own trace (linked to the producer), per the OTEL messaging
        // conventions — the queue wait must not be absorbed into the producer's trace. A synchronously
        // received message is handled in-process, so it stays a child of the current trace.
        $span = $this->traceHandler
            ? $tracer->span($spanName, $kind, $attributes, $links, $isWorkerConsumed ? false : null)
            : null;

        if (!$isReceived) {
            $envelope = $this->injectContext($envelope);
        }

        $startedAt = $span === null ? hrtime(true) : null;
        $errorType = null;
        $handlingFailure = false;
        $resultEnvelope = null;

        try {
            $resultEnvelope = $stack->next()->handle($envelope, $stack);
        } catch (Throwable $e) {
            // HandleMessageMiddleware wraps handler exceptions in HandlerFailedException (and
            // dispatch_after_current_bus in DelayedMessageHandlingException) — record the actual
            // failures, not the wrapper every failed message shares.
            $causes = $e instanceof WrappedExceptionsInterface
                ? array_values($e->getWrappedExceptions(recursive: true))
                : [];
            $handlingFailure = $causes !== [];

            if ($causes === []) {
                $causes = [$e];
            }

            $errorType = $causes[0]::class;

            if ($span !== null) {
                foreach ($causes as $cause) {
                    $span->recordException($cause, new DateTimeImmutable());
                }

                $span->setAttribute(SemConvAttributes::ERROR_TYPE, $errorType);
                $span->setStatus(SpanStatus::error($causes[0]->getMessage()));
            }

            throw $e;
        } finally {
            $destination = !$isReceived && $resultEnvelope !== null ? self::sentDestination($resultEnvelope) : null;

            if ($span !== null) {
                if (!$isReceived && $resultEnvelope !== null) {
                    $this->finalizeProducerSpan($span, $resultEnvelope, $destination);
                }

                $tracer->complete($span);
            }

            if ($meter !== null) {
                $counterAttributes = $metricAttributes;

                if ($destination !== null) {
                    $counterAttributes[SemConvAttributes::MESSAGING_DESTINATION_NAME] = $destination;
                }

                if ($errorType !== null) {
                    $counterAttributes[SemConvAttributes::ERROR_TYPE] = $errorType;
                }

                // A dispatch without a SentStamp was handled in-process, never sent to a broker; a failed
                // dispatch counts as a send attempt unless the failure happened in handling.
                $wasSent = $resultEnvelope !== null
                    ? $resultEnvelope->last(SentStamp::class) !== null
                    : !$handlingFailure;

                if ($isReceived || $wasSent) {
                    $meter->createCounter(
                        $isReceived
                            ? SemConvMetrics::MESSAGING_CLIENT_CONSUMED_MESSAGES
                            : SemConvMetrics::MESSAGING_CLIENT_SENT_MESSAGES,
                        '{message}',
                        $isReceived
                            ? 'Number of messages delivered to the application'
                            : 'Number of messages sent to the broker',
                    )->add(1, $counterAttributes);
                }
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

        return $resultEnvelope;
    }

    /**
     * Routing to a transport happens deeper in the middleware stack, so the bus name, transport message
     * id and destination only exist on the envelope the stack returns — the producer span is finalized
     * from it, mirroring how the HTTP request span picks up the route once it is known.
     */
    private function finalizeProducerSpan(Span $span, Envelope $result, ?string $destination): void
    {
        $busNameStamp = $result->last(BusNameStamp::class);

        if ($busNameStamp instanceof BusNameStamp) {
            $span->setAttribute(MessengerAttributes::ATTR_BUS, $busNameStamp->getBusName());
        }

        $transportIdStamp = $result->last(TransportMessageIdStamp::class);

        if ($transportIdStamp instanceof TransportMessageIdStamp) {
            $span->setAttribute(SemConvAttributes::MESSAGING_MESSAGE_ID, (string) $transportIdStamp->getId());
        }

        if ($destination !== null) {
            $span->setAttribute(SemConvAttributes::MESSAGING_DESTINATION_NAME, $destination);

            if ($this->messageNaming === MessageNaming::Transport) {
                $span->rename("send {$destination}");
            }
        }
    }

    /**
     * A message routed to multiple transports has no single semconv destination, so only a sole
     * SentStamp yields one.
     */
    private static function sentDestination(Envelope $result): ?string
    {
        $sentStamps = array_values($result->all(SentStamp::class));

        if (count($sentStamps) !== 1) {
            return null;
        }

        return $sentStamps[0]->getSenderAlias();
    }

    /**
     * @param class-string $fqcn
     */
    private static function shortClassName(string $fqcn): string
    {
        $position = strrpos($fqcn, '\\');

        return $position === false ? $fqcn : substr($fqcn, $position + 1);
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
