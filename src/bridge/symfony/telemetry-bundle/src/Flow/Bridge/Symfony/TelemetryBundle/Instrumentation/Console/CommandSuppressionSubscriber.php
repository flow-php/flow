<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

use function preg_match;

/**
 * Suppresses tracing for the entire execution of the configured commands (long-running worker commands),
 * so their between-work activity (transport polls, idle-tick co-listeners, …) does not surface. A
 * suppression flag is attached to the telemetry context when a matching command starts and detached when it
 * terminates; the SuppressingSampler then drops any span created while it is set. Instrumentation that must
 * still be traced lifts the suppression around its own work (e.g. the messenger middleware per message).
 *
 * The flag is attached above ConsoleSpanSubscriber's COMMAND priority (10000) so the command's own console
 * span is created non-recording, and detached below its TERMINATE priority (-10000) so that span completes
 * first.
 */
final class CommandSuppressionSubscriber implements EventSubscriberInterface
{
    private const SET_PRIORITY = 20000;

    private const CLEAR_PRIORITY = -20000;

    private ?Scope $scope = null;

    /**
     * @param array<string> $suppressCommands
     */
    public function __construct(
        private readonly ContextStorage $contextStorage,
        private readonly array $suppressCommands,
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
        $name = $event->getCommand()?->getName();

        if ($name !== null && $this->shouldSuppress($name)) {
            $this->scope = $this->contextStorage->attach($this->contextStorage->current()->withSuppressedTracing());
        }
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
    {
        $this->scope?->detach();
        $this->scope = null;
    }

    private function shouldSuppress(string $commandName): bool
    {
        foreach ($this->suppressCommands as $pattern) {
            if ($this->matchesPattern($commandName, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $command, string $pattern): bool
    {
        $result = @preg_match($pattern, $command);

        if ($result !== false) {
            return (bool) $result;
        }

        return $command === $pattern;
    }
}
