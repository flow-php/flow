<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use DateTimeImmutable;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\Console\Event\ConsoleSignalEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Throwable;

use function array_key_last;
use function array_map;
use function array_pop;

final class ConsoleSpanSubscriber implements EventSubscriberInterface
{
    /** @var array<CommandExclusionRule> */
    private readonly array $excludeRules;

    /**
     * Commands nested via Application::run() dispatch balanced COMMAND/TERMINATE pairs, so entries are
     * stacked — a nested command's TERMINATE completes its own span, not the enclosing command's.
     * Excluded commands push null to keep the pairs balanced.
     *
     * @var array<null|array{span: Span, tracer: Tracer, scope: Scope, error: null|Throwable}>
     */
    private array $stack = [];

    /**
     * @param array<string> $excludeCommands
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        array $excludeCommands = [],
    ) {
        $this->excludeRules = array_map(
            static fn(string $pattern): CommandExclusionRule => new CommandExclusionRule($pattern),
            $excludeCommands,
        );
    }

    public static function getSubscribedEvents(): array
    {
        return [
            ConsoleEvents::COMMAND => ['onCommand', 10000],
            ConsoleEvents::ERROR => ['onError', 0],
            ConsoleEvents::TERMINATE => ['onTerminate', -10000],
            ConsoleEvents::SIGNAL => ['onSignal', 0],
        ];
    }

    public function onCommand(ConsoleCommandEvent $event): void
    {
        $command = $event->getCommand();
        $commandName = $command?->getName() ?? 'unknown';

        if (!$this->shouldTrace($commandName)) {
            $this->stack[] = null;

            return;
        }

        $tracer = $this->telemetry->tracer('flow.symfony.console', PackageVersion::get('symfony/console'));

        $attributes = [
            ConsoleAttributes::ATTR_COMMAND_NAME => $commandName,
        ];

        if ($command !== null) {
            $attributes[ConsoleAttributes::ATTR_COMMAND_CLASS] = $command::class;
        }

        $span = $tracer->span($commandName, SpanKind::INTERNAL, $attributes);

        // activated: a command span is a logical scope - everything the command does belongs under it
        $this->stack[] = [
            'span' => $span,
            'tracer' => $tracer,
            'scope' => $tracer->activate($span),
            'error' => null,
        ];
    }

    public function onError(ConsoleErrorEvent $event): void
    {
        $index = array_key_last($this->stack);

        if ($index === null || $this->stack[$index] === null) {
            return;
        }

        $this->stack[$index]['error'] = $event->getError();
        $this->stack[$index]['span']->recordException($event->getError(), new DateTimeImmutable());
    }

    public function onSignal(ConsoleSignalEvent $event): void
    {
        $index = array_key_last($this->stack);

        if ($index === null || $this->stack[$index] === null) {
            return;
        }

        $this->stack[$index]['span']->setAttribute(ConsoleAttributes::ATTR_COMMAND_SIGNAL, $event->getHandlingSignal());
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
    {
        if ($this->stack === []) {
            return;
        }

        $entry = array_pop($this->stack);

        if ($entry === null) {
            return;
        }

        $span = $entry['span'];
        $exitCode = $event->getExitCode();
        $span->setAttribute(SemConvAttributes::PROCESS_EXIT_CODE, $exitCode);

        // OTEL spec: instrumentation leaves the status Unset on success; only a non-zero exit is an error.
        // When an exception caused the failure, error.type is its class; the exit code is the fallback for
        // exception-less failures.
        if ($exitCode !== 0) {
            $error = $entry['error'];
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $error !== null ? $error::class : (string) $exitCode);
            $span->setStatus(SpanStatus::error($error !== null ? $error->getMessage() : "Exit code: {$exitCode}"));
        }

        $entry['scope']->detach();
        $entry['tracer']->complete($span);
    }

    private function shouldTrace(string $commandName): bool
    {
        foreach ($this->excludeRules as $rule) {
            if ($rule->matches($commandName)) {
                return false;
            }
        }

        return true;
    }
}
