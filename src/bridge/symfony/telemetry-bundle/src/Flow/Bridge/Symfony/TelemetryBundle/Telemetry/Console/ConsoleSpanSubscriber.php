<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Telemetry\Console;

use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\{Span, SpanKind, SpanStatus, Tracer};
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\{ConsoleCommandEvent, ConsoleErrorEvent, ConsoleSignalEvent, ConsoleTerminateEvent};
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

final class ConsoleSpanSubscriber implements EventSubscriberInterface
{
    private ?Span $span = null;

    private ?Tracer $tracer = null;

    /**
     * @param array<string> $excludeCommands
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly array $excludeCommands = [],
    ) {
    }

    public static function getSubscribedEvents() : array
    {
        return [
            ConsoleEvents::COMMAND => ['onCommand', 10000],
            ConsoleEvents::ERROR => ['onError', 0],
            ConsoleEvents::TERMINATE => ['onTerminate', -10000],
            ConsoleEvents::SIGNAL => ['onSignal', 0],
        ];
    }

    public function onCommand(ConsoleCommandEvent $event) : void
    {
        $command = $event->getCommand();
        $commandName = $command?->getName() ?? 'unknown';

        if (!$this->shouldTrace($commandName)) {
            return;
        }

        $this->tracer = $this->telemetry->tracer('flow.symfony.console');

        $attributes = [
            'command.name' => $commandName,
        ];

        if ($command !== null) {
            $attributes['command.class'] = $command::class;
        }

        $this->span = $this->tracer->span(
            $commandName,
            SpanKind::INTERNAL,
            $attributes,
        );
    }

    public function onError(ConsoleErrorEvent $event) : void
    {
        if ($this->span === null) {
            return;
        }

        $this->span->recordException($event->getError(), new \DateTimeImmutable());
    }

    public function onSignal(ConsoleSignalEvent $event) : void
    {
        if ($this->span === null) {
            return;
        }

        $this->span->setAttribute('process.signal', $event->getHandlingSignal());
    }

    public function onTerminate(ConsoleTerminateEvent $event) : void
    {
        if ($this->span === null || $this->tracer === null) {
            return;
        }

        $exitCode = $event->getExitCode();
        $this->span->setAttribute('process.exit_code', $exitCode);

        if ($exitCode === 0) {
            $this->span->setStatus(SpanStatus::ok());
        } else {
            $this->span->setStatus(SpanStatus::error("Exit code: {$exitCode}"));
        }

        $this->tracer->complete($this->span);

        $this->span = null;
        $this->tracer = null;
    }

    private function matchesPattern(string $command, string $pattern) : bool
    {
        if (\str_starts_with($pattern, '/') && \str_ends_with($pattern, '/')) {
            return (bool) \preg_match($pattern, $command);
        }

        return $command === $pattern;
    }

    private function shouldTrace(string $commandName) : bool
    {
        foreach ($this->excludeCommands as $pattern) {
            if ($this->matchesPattern($commandName, $pattern)) {
                return false;
            }
        }

        return true;
    }
}
