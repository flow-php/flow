<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Logger;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\Severity;
use Symfony\Component\Console\Formatter\OutputFormatter;
use Symfony\Component\Console\Output\OutputInterface;

use function count;
use function implode;
use function is_scalar;
use function json_encode;

final class ConsoleOutputLogProcessor implements LogProcessor
{
    private ?OutputInterface $output = null;

    public function __construct(
        private readonly ConsoleVerbosityLevels $levels,
    ) {}

    public function clearOutput(): void
    {
        $this->output = null;
    }

    public function flush(): bool
    {
        return true;
    }

    public function process(LogEntry $entry): void
    {
        if ($this->output === null) {
            return;
        }

        if (!$entry->record->severity->isAtLeast($this->levels->thresholdFor($this->output->getVerbosity()))) {
            return;
        }

        $this->output->writeln($this->format($entry), OutputInterface::VERBOSITY_QUIET);
    }

    public function setOutput(OutputInterface $output): void
    {
        $this->output = $output;
    }

    public function shutdown(): void {}

    /**
     * @param array<string, array<array-key, mixed>|bool|float|int|string> $attributes
     */
    private function formatAttributes(array $attributes): string
    {
        $parts = [];

        foreach ($attributes as $key => $value) {
            $rendered = is_scalar($value) ? (string) $value : (json_encode($value) ?: '');
            $parts[] = $key . '=' . OutputFormatter::escape($rendered);
        }

        return '{' . implode(', ', $parts) . '}';
    }

    private function format(LogEntry $entry): string
    {
        $severity = $entry->record->severity;
        $level = match ($severity) {
            Severity::FATAL, Severity::ERROR => '<error>' . $severity->name() . '</error>',
            Severity::WARN => '<comment>' . $severity->name() . '</comment>',
            Severity::INFO => '<info>' . $severity->name() . '</info>',
            default => $severity->name(),
        };

        $line =
            $entry->timestamp->format('H:i:s.v') . ' [' . $level . '] ' . OutputFormatter::escape($entry->record->body);

        $attributes = $entry->record->attributes->normalize();

        if (count($attributes) > 0) {
            $line .= ' ' . $this->formatAttributes($attributes);
        }

        return $line;
    }
}
