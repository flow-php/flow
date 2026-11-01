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
 * Priorities wrap ConsoleSpanSubscriber (±10000); scopes stack per COMMAND/TERMINATE for nested commands.
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
