<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\Worker;

use function class_exists;
use function implode;

final class WorkerReceiveCycleSubscriber implements EventSubscriberInterface
{
    private const COMPLETE_PRIORITY = 1024;

    private const OPEN_PRIORITY = -2048;

    private ?Span $cycleSpan = null;

    public function __construct(
        private readonly Telemetry $telemetry,
    ) {}

    public static function getSubscribedEvents(): array
    {
        $events = [
            WorkerStartedEvent::class => 'onWorkerStarted',
            WorkerRunningEvent::class => [
                ['completeCycle', self::COMPLETE_PRIORITY],
                ['openCycle', self::OPEN_PRIORITY],
            ],
            WorkerStoppedEvent::class => ['onWorkerStopped', self::COMPLETE_PRIORITY],
        ];

        if (class_exists(ConsoleErrorEvent::class)) {
            $events[ConsoleEvents::ERROR] = 'onConsoleError';
        }

        return $events;
    }

    public function onWorkerStarted(WorkerStartedEvent $event): void
    {
        $this->cycleSpan = $this->openSpan($event->getWorker());
    }

    public function completeCycle(WorkerRunningEvent $event): void
    {
        $cycleSpan = $this->cycleSpan;

        if ($cycleSpan === null) {
            return;
        }

        $cycleSpan->setAttribute('messaging.symfony.worker.idle', $event->isWorkerIdle());
        $this->tracer()->complete($cycleSpan);
        $this->cycleSpan = null;
        $this->telemetry->flush();
    }

    public function openCycle(WorkerRunningEvent $event): void
    {
        $this->cycleSpan = $this->openSpan($event->getWorker());
    }

    public function onWorkerStopped(WorkerStoppedEvent $event): void
    {
        $cycleSpan = $this->cycleSpan;

        if ($cycleSpan === null) {
            return;
        }

        $this->tracer()->complete($cycleSpan);
        $this->cycleSpan = null;
        $this->telemetry->flush();
    }

    public function onConsoleError(ConsoleErrorEvent $event): void
    {
        $cycleSpan = $this->cycleSpan;

        if ($cycleSpan === null) {
            return;
        }

        $error = $event->getError();
        $cycleSpan->recordException($error, new DateTimeImmutable());
        $cycleSpan->setAttribute('error.type', $error::class);
        $cycleSpan->setStatus(SpanStatus::error($error->getMessage()));
        $this->tracer()->complete($cycleSpan);
        $this->cycleSpan = null;
        $this->telemetry->flush();
    }

    private function openSpan(Worker $worker): Span
    {
        $attributes = [
            'messaging.system' => 'symfony_messenger',
            'messaging.operation.type' => 'receive',
            'messaging.operation.name' => 'receive',
        ];

        /** @var list<string> $transports */
        $transports = $worker->getMetadata()->getTransportNames();

        if ($transports !== []) {
            $attributes['messaging.symfony.worker.transports'] = implode(',', $transports);
        }

        return $this->tracer()->span('messenger.receive', SpanKind::CONSUMER, $attributes, [], false);
    }

    private function tracer(): Tracer
    {
        return $this->telemetry->tracer('flow.symfony.messenger', PackageVersion::get('symfony/messenger'));
    }
}
