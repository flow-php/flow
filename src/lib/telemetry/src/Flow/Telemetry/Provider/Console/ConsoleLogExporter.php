<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, Severity};
use Flow\Telemetry\Resource as TelemetryResource;
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Console exporter for logs with ASCII table visualization.
 *
 * Outputs log records to the console in a framed table format with
 * optional ANSI colors based on severity level.
 * Table width is calculated dynamically based on content.
 */
final readonly class ConsoleLogExporter implements LogExporter
{
    private const int MIN_WIDTH = 40;

    private const int SEVERITY_WIDTH = 5;

    private const int TIMESTAMP_WIDTH = 26;

    private const int TRACE_WIDTH = 17;

    private ConsoleOutput $output;

    /**
     * @param bool $colors Whether to use ANSI colors
     * @param null|int $maxBodyLength Maximum length for body+attributes column (null = no limit)
     * @param null|resource $outputStream Output stream (default: STDOUT)
     */
    public function __construct(
        bool $colors = true,
        private ?int $maxBodyLength = null,
        mixed $outputStream = null,
        private ConsoleLogOptions $options = new ConsoleLogOptions(),
    ) {
        /** @var null|resource $outputStream */
        $this->output = new ConsoleOutput($colors, $outputStream);
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function export(array $entries) : bool
    {
        if (\count($entries) === 0) {
            return true;
        }

        $formattedRecords = $this->formatEntries($entries);
        $bodyWidth = $this->calculateBodyWidth($formattedRecords);
        $hasTrace = $this->hasAnySpanContext($entries);
        $resourceLines = $this->buildResourceLines($entries[0]->resource);
        $scopeLine = $this->buildScopeLine($entries[0]);
        $width = $this->calculateTotalWidth($bodyWidth, $hasTrace);

        foreach ($resourceLines as $resourceLine) {
            $resourceWidth = \mb_strlen($this->output->stripColors($resourceLine)) + 4;
            $width = \max($width, $resourceWidth);
        }

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold('LOGS'), $width) . PHP_EOL;

        if (\count($resourceLines) > 0) {
            foreach ($resourceLines as $resourceLine) {
                $buffer .= $this->output->row($resourceLine, $width) . PHP_EOL;
            }
        }

        if ($scopeLine !== null) {
            $buffer .= $this->output->row($scopeLine, $width) . PHP_EOL;
        }

        $buffer .= $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->buildHeaderLine($bodyWidth, $hasTrace), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        foreach ($formattedRecords as $record) {
            $buffer .= $this->output->row($this->buildLine($record, $bodyWidth, $hasTrace), $width) . PHP_EOL;
        }

        $buffer .= $this->output->border($width);

        $this->output->write($buffer);

        return true;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }

    private function buildHeaderLine(int $bodyWidth, bool $hasTrace) : string
    {
        $timestamp = $this->output->pad('Timestamp', self::TIMESTAMP_WIDTH);
        $level = $this->output->pad('Level', self::SEVERITY_WIDTH);
        $message = $this->output->pad('Message', $bodyWidth);

        $line = $this->output->bold($timestamp) . ' | ';
        $line .= $this->output->bold($level) . ' | ';
        $line .= $this->output->bold($message);

        if ($hasTrace) {
            $trace = $this->output->pad('Trace', self::TRACE_WIDTH);
            $line .= ' | ' . $this->output->bold($trace);
        }

        return $line;
    }

    /**
     * @param array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int} $record
     */
    private function buildLine(array $record, int $bodyWidth, bool $hasTrace) : string
    {
        $timestamp = $this->output->pad($record['timestamp'], self::TIMESTAMP_WIDTH);
        $severity = $this->output->pad($record['severity'], self::SEVERITY_WIDTH);
        $body = $this->output->truncate($record['body'], $bodyWidth);
        $body = $this->output->pad($body, $bodyWidth);

        $line = $this->output->gray($timestamp) . ' | ';
        $line .= $this->colorBySeverity($severity, $record['severityRaw']) . ' | ';
        $line .= $body;

        if ($this->options->showDroppedAttributeCount && $record['droppedAttributeCount'] > 0) {
            $line .= ' ' . $this->output->yellow('[dropped:' . $record['droppedAttributeCount'] . ']');
        }

        if ($hasTrace) {
            $trace = $this->output->pad($record['trace'], self::TRACE_WIDTH);
            $line .= ' | ' . ($record['trace'] !== '' ? $this->output->dim($trace) : $trace);
        }

        return $line;
    }

    /**
     * @return array<string>
     */
    private function buildResourceLines(TelemetryResource $resource) : array
    {
        if ($resource->isEmpty()) {
            return [];
        }

        if (!$this->options->showResourceAttributes) {
            $parts = [];
            $serviceName = $resource->get('service.name');

            if (\is_string($serviceName)) {
                $parts[] = $serviceName;
            }

            $serviceVersion = $resource->get('service.version');

            if (\is_string($serviceVersion)) {
                $parts[] = 'v' . $serviceVersion;
            }

            if (\count($parts) === 0) {
                return [];
            }

            return ['Resource: ' . $this->output->dim(\implode(' ', $parts))];
        }

        $lines = [];
        $lines[] = $this->output->bold('Resource:');

        $attributes = $resource->all();
        $maxKeyLength = 0;

        foreach (\array_keys($attributes) as $key) {
            $maxKeyLength = \max($maxKeyLength, \mb_strlen($key));
        }

        foreach ($attributes as $key => $value) {
            $keyStr = $this->output->pad($key, $maxKeyLength);
            $valueStr = $this->output->formatValue($value);
            $lines[] = '  ' . $this->output->cyan($keyStr) . ' = ' . $valueStr;
        }

        return $lines;
    }

    private function buildScopeLine(LogEntry $entry) : ?string
    {
        if (!$this->options->showInstrumentationScope) {
            return null;
        }

        $scope = $entry->scope;

        return 'Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version);
    }

    /**
     * @param array<array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int}> $records
     */
    private function calculateBodyWidth(array $records) : int
    {
        $maxLength = 0;

        foreach ($records as $record) {
            $maxLength = \max($maxLength, \mb_strlen($record['body']));
        }

        if ($this->maxBodyLength !== null) {
            return \min($maxLength, $this->maxBodyLength);
        }

        return $maxLength;
    }

    private function calculateTotalWidth(int $bodyWidth, bool $hasTrace) : int
    {
        $width = 4;
        $width += self::TIMESTAMP_WIDTH;
        $width += 3;
        $width += self::SEVERITY_WIDTH;
        $width += 3;
        $width += $bodyWidth;

        if ($hasTrace) {
            $width += 3;
            $width += self::TRACE_WIDTH;
        }

        return \max(self::MIN_WIDTH, $width);
    }

    private function colorBySeverity(string $text, Severity $severity) : string
    {
        return match ($severity) {
            Severity::TRACE => $this->output->dim($text),
            Severity::DEBUG => $this->output->gray($text),
            Severity::INFO => $this->output->green($text),
            Severity::WARN => $this->output->yellow($text),
            Severity::ERROR => $this->output->red($text),
            Severity::FATAL => $this->output->bold($this->output->red($text)),
        };
    }

    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    private function formatAttributes(array $attributes) : string
    {
        if (\count($attributes) === 0) {
            return '';
        }

        $parts = [];

        foreach ($attributes as $key => $value) {
            $parts[] = $key . ': ' . $this->output->formatValue($this->normalizeValue($value));
        }

        return '{' . \implode(', ', $parts) . '}';
    }

    /**
     * @param array<LogEntry> $entries
     *
     * @return array<array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int}>
     */
    private function formatEntries(array $entries) : array
    {
        $formatted = [];

        foreach ($entries as $entry) {
            $body = $entry->record->body;
            $attrs = $this->formatAttributes($entry->record->attributes->normalize());

            if ($attrs !== '') {
                $body .= ' ' . $attrs;
            }

            $trace = '';

            if ($entry->spanContext !== null) {
                $trace = \mb_substr($entry->spanContext->traceId->toHex(), 0, 8) . '/' . \mb_substr($entry->spanContext->spanId->toHex(), 0, 8);
            }

            $timestamp = $entry->timestamp->format('Y-m-d H:i:s.u');

            if ($this->options->showObservedTimestamp && $entry->record->observedTimestamp !== null) {
                $timestamp .= ' (obs: ' . $entry->record->observedTimestamp->format('H:i:s.u') . ')';
            }

            $formatted[] = [
                'timestamp' => $timestamp,
                'severity' => $entry->record->severity->name,
                'severityRaw' => $entry->record->severity,
                'body' => $body,
                'trace' => $trace,
                'droppedAttributeCount' => $entry->droppedAttributeCount,
            ];
        }

        return $formatted;
    }

    /**
     * @param array<LogEntry> $entries
     */
    private function hasAnySpanContext(array $entries) : bool
    {
        foreach ($entries as $entry) {
            if ($entry->spanContext !== null) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string $value
     *
     * @return array<bool|float|int|string>|bool|float|int|string
     */
    private function normalizeValue(mixed $value) : array|bool|float|int|string
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value->format(\DateTimeInterface::RFC3339_EXTENDED);
        }

        if (\is_array($value)) {
            return \array_map(
                static fn ($item) => $item instanceof \DateTimeImmutable
                    ? $item->format(\DateTimeInterface::RFC3339_EXTENDED)
                    : $item,
                $value
            );
        }

        return $value;
    }
}
