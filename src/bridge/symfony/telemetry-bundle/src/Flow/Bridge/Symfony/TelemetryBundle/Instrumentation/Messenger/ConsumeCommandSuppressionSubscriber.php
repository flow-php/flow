<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Command\ConsumeMessagesCommand;

/**
 * Suppresses tracing for the whole messenger:consume command so the worker process emits only per-message
 * handler traces. Suppression is a context value attached once at command start and detached at
 * termination — nothing the worker loop does (transport poll, co-listeners) records, because the
 * SuppressingSampler drops spans while the active context is suppressed. TracingMiddleware lifts the
 * suppression per message so handler work is still traced.
 *
 * The scope is attached above ConsoleSpanSubscriber's COMMAND priority (10000) so the long-lived
 * messenger:consume console span is created non-recording, and detached below its TERMINATE priority
 * (-10000) so that span completes first.
 */
final class ConsumeCommandSuppressionSubscriber implements EventSubscriberInterface
{
    private const SET_PRIORITY = 20000;

    private const CLEAR_PRIORITY = -20000;

    private ?Scope $scope = null;

    public function __construct(
        private readonly ContextStorage $contextStorage,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            ConsoleEvents::COMMAND => ['onCommand', self::SET_PRIORITY],
            ConsoleEvents::TERMINATE => ['onTerminate', self::CLEAR_PRIORITY],
        ];
    }

    public function onCommand(ConsoleCommandEvent $event): void
    {
        if ($event->getCommand() instanceof ConsumeMessagesCommand) {
            $this->scope = $this->contextStorage->attach($this->contextStorage->current()->withSuppressedTracing());
        }
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
    {
        $this->scope?->detach();
        $this->scope = null;
    }
}
