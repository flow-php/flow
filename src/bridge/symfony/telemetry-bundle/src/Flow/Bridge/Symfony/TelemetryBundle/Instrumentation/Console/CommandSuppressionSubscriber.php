<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

use function array_map;
use function array_pop;

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
 *
 * Scopes are stacked per COMMAND/TERMINATE pair (Symfony dispatches them balanced, also for commands nested
 * via Application::run()), so a nested command's TERMINATE detaches its own entry instead of tearing down
 * the suppression of the enclosing command.
 */
final class CommandSuppressionSubscriber implements EventSubscriberInterface
{
    private const SET_PRIORITY = 20000;

    private const CLEAR_PRIORITY = -20000;

    /** @var array<CommandExclusionRule> */
    private readonly array $suppressRules;

    /**
     * @var array<null|Scope>
     */
    private array $scopes = [];

    /**
     * @param array<string> $suppressCommands
     */
    public function __construct(
        private readonly ContextStorage $contextStorage,
        array $suppressCommands,
    ) {
        $this->suppressRules = array_map(
            static fn(string $pattern): CommandExclusionRule => new CommandExclusionRule($pattern),
            $suppressCommands,
        );
    }

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
            $this->scopes[] = $this->contextStorage->attach($this->contextStorage->current()->withSuppressedTracing());

            return;
        }

        $this->scopes[] = null;
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
    {
        if ($this->scopes === []) {
            return;
        }

        array_pop($this->scopes)?->detach();
    }

    private function shouldSuppress(string $commandName): bool
    {
        foreach ($this->suppressRules as $rule) {
            if ($rule->matches($commandName)) {
                return true;
            }
        }

        return false;
    }
}
