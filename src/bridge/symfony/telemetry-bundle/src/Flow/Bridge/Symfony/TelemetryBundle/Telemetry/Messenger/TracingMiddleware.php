<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Messenger;

use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus};
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Middleware\{MiddlewareInterface, StackInterface};
use Symfony\Component\Messenger\Stamp\{BusNameStamp, ReceivedStamp, TransportMessageIdStamp};

final readonly class TracingMiddleware implements MiddlewareInterface
{
    public function __construct(
        private Telemetry $telemetry,
    ) {
    }

    public function handle(Envelope $envelope, StackInterface $stack) : Envelope
    {
        $tracer = $this->telemetry->tracer('flow.symfony.messenger');

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
            'messaging.destination' => $busName,
            'messaging.message.class' => $messageClass,
            'messaging.operation' => $operation,
        ];

        if ($receivedStamp instanceof ReceivedStamp) {
            $attributes['messaging.transport'] = $receivedStamp->getTransportName();
        }

        if ($transportIdStamp instanceof TransportMessageIdStamp) {
            $attributes['messaging.message.id'] = (string) $transportIdStamp->getId();
        }

        $span = $tracer->span($spanName, $kind, $attributes);

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

    private function getShortClassName(string $className) : string
    {
        $parts = \explode('\\', $className);

        return \end($parts);
    }
}
