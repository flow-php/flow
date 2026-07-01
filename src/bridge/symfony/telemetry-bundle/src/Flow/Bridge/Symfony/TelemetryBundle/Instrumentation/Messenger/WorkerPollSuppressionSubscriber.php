<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;

/**
 * Suppresses instrumentation during the worker's receive loop when the worker cycle span is not traced,
 * so the transport's poll/claim operations (Doctrine DBAL, HTTP client, ...) do not surface as orphan
 * root spans once per poll. TracingMiddleware lifts the suppression around message handling so handler
 * work is still traced.
 *
 * The suppression flag lives on the telemetry context, which Symfony resets after every non-idle
 * WorkerRunningEvent (ResetServicesListener, priority -1024). It is therefore detached before that reset
 * (priority above -1024) and reattached after it (priority below -1024).
 */
final class WorkerPollSuppressionSubscriber implements EventSubscriberInterface
{
    private const RESUME_PRIORITY = 1024;

    private const SUPPRESS_PRIORITY = -2048;

    private ?Scope $scope = null;

    public function __construct(
        private readonly ContextStorage $contextStorage,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerStartedEvent::class => 'onWorkerStarted',
            WorkerRunningEvent::class => [
                ['onWorkerRunningResume', self::RESUME_PRIORITY],
                ['onWorkerRunningSuppress', self::SUPPRESS_PRIORITY],
            ],
            WorkerStoppedEvent::class => ['onWorkerStopped', self::RESUME_PRIORITY],
        ];
    }

    public function onWorkerStarted(WorkerStartedEvent $event): void
    {
        $this->suppress();
    }

    public function onWorkerRunningResume(WorkerRunningEvent $event): void
    {
        $this->resume();
    }

    public function onWorkerRunningSuppress(WorkerRunningEvent $event): void
    {
        $this->suppress();
    }

    public function onWorkerStopped(WorkerStoppedEvent $event): void
    {
        $this->resume();
    }

    private function suppress(): void
    {
        if ($this->scope !== null) {
            return;
        }

        $this->scope = $this->contextStorage->attach($this->contextStorage->current()->withSuppressedTracing());
    }

    private function resume(): void
    {
        $this->scope?->detach();
        $this->scope = null;
    }
}
