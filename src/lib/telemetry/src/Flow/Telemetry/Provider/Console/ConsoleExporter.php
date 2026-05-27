<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource as TelemetryResource;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanStatusCode;

use function abs;
use function array_keys;
use function array_merge;
use function count;
use function implode;
use function is_float;
use function is_scalar;
use function is_string;
use function json_encode;
use function max;
use function mb_strlen;
use function mb_substr;
use function min;
use function number_format;
use function sprintf;
use function strtoupper;
use function substr;

/**
 * Unified console exporter for logs, metrics, and spans.
 *
 * Outputs telemetry to the console in human-readable formatted tables with
 * optional ANSI colors. Dispatches on batch type and renders accordingly.
 */
final readonly class ConsoleExporter implements Exporter
{
    private const int LOG_MIN_WIDTH = 40;

    private const int LOG_SEVERITY_WIDTH = 5;

    private const int LOG_TIMESTAMP_WIDTH = 26;

    private const int LOG_TRACE_WIDTH = 17;

    private const int METRIC_MIN_WIDTH = 40;

    private const int SPAN_MIN_WIDTH = 40;

    private ConsoleOutput $output;

    /**
     * @param bool $colors Whether to use ANSI colors
     * @param null|int $maxLogBodyLength Maximum length for log body+attributes column (null = no limit)
     * @param null|resource $outputStream Output stream (default: STDOUT)
     */
    public function __construct(
        bool $colors = true,
        private ?int $maxLogBodyLength = null,
        mixed $outputStream = null,
        private ConsoleLogOptions $logOptions = new ConsoleLogOptions(),
        private ConsoleMetricOptions $metricOptions = new ConsoleMetricOptions(),
        private ConsoleSpanOptions $spanOptions = new ConsoleSpanOptions(),
    ) {
        $this->output = new ConsoleOutput($colors, $outputStream);
    }

    public function export(Signals $signal): bool
    {
        return match ($signal->type) {
            SignalType::LOGS => $this->exportLogs($signal->allLogs()),
            SignalType::METRICS => $this->exportMetrics($signal->allMetrics()),
            SignalType::TRACES => $this->exportSpans($signal->allSpans()),
        };
    }

    public function shutdown(): void {}

    private function appendMetricExemplarInfo(string &$line, Metric $metric): void
    {
        if (count($metric->exemplars) === 0) {
            return;
        }

        if ($this->metricOptions->showAllExemplars) {
            foreach ($metric->exemplars as $exemplar) {
                $traceIdShort = substr($exemplar->traceId->toHex(), 0, 8);
                $spanIdShort = substr($exemplar->spanId->toHex(), 0, 8);
                $line .= '  ' . $this->output->dim('[trace:' . $traceIdShort . '.. span:' . $spanIdShort . '..]');
            }
        } else {
            $exemplar = $metric->exemplars[0];
            $traceIdShort = substr($exemplar->traceId->toHex(), 0, 8);
            $spanIdShort = substr($exemplar->spanId->toHex(), 0, 8);
            $line .= '  ' . $this->output->dim('[trace:' . $traceIdShort . '.. span:' . $spanIdShort . '..]');
        }
    }

    /**
     * @return array<string>
     */
    private function buildFullResourceLines(TelemetryResource $resource): array
    {
        $lines = [];
        $lines[] = $this->output->bold('Resource:');

        $attributes = $resource->all();
        $maxKeyLength = 0;

        foreach (array_keys($attributes) as $key) {
            $maxKeyLength = max($maxKeyLength, mb_strlen($key));
        }

        foreach ($attributes as $key => $value) {
            $keyStr = $this->output->pad($key, $maxKeyLength);
            $valueStr = $this->output->formatValue($value);
            $lines[] = '  ' . $this->output->cyan($keyStr) . ' = ' . $valueStr;
        }

        return $lines;
    }

    private function buildLogHeaderLine(int $bodyWidth, bool $hasTrace): string
    {
        $timestamp = $this->output->pad('Timestamp', self::LOG_TIMESTAMP_WIDTH);
        $level = $this->output->pad('Level', self::LOG_SEVERITY_WIDTH);
        $message = $this->output->pad('Message', $bodyWidth);

        $line = $this->output->bold($timestamp) . ' | ';
        $line .= $this->output->bold($level) . ' | ';
        $line .= $this->output->bold($message);

        if ($hasTrace) {
            $trace = $this->output->pad('Trace', self::LOG_TRACE_WIDTH);
            $line .= ' | ' . $this->output->bold($trace);
        }

        return $line;
    }

    /**
     * @param array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int} $record
     */
    private function buildLogLine(array $record, int $bodyWidth, bool $hasTrace): string
    {
        $timestamp = $this->output->pad($record['timestamp'], self::LOG_TIMESTAMP_WIDTH);
        $severity = $this->output->pad($record['severity'], self::LOG_SEVERITY_WIDTH);
        $body = $this->output->truncate($record['body'], $bodyWidth);
        $body = $this->output->pad($body, $bodyWidth);

        $line = $this->output->gray($timestamp) . ' | ';
        $line .= $this->colorBySeverity($severity, $record['severityRaw']) . ' | ';
        $line .= $body;

        if ($this->logOptions->showDroppedAttributeCount && $record['droppedAttributeCount'] > 0) {
            $line .= ' ' . $this->output->yellow('[dropped:' . $record['droppedAttributeCount'] . ']');
        }

        if ($hasTrace) {
            $trace = $this->output->pad($record['trace'], self::LOG_TRACE_WIDTH);
            $line .= ' | ' . ($record['trace'] !== '' ? $this->output->dim($trace) : $trace);
        }

        return $line;
    }

    /**
     * @return array<string>
     */
    private function buildLogResourceLines(TelemetryResource $resource): array
    {
        if ($resource->isEmpty()) {
            return [];
        }

        if (!$this->logOptions->showResourceAttributes) {
            return $this->buildShortResourceLines($resource);
        }

        return $this->buildFullResourceLines($resource);
    }

    private function buildLogScopeLine(LogEntry $entry): ?string
    {
        if (!$this->logOptions->showInstrumentationScope) {
            return null;
        }

        $scope = $entry->scope;

        return 'Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version);
    }

    /**
     * @param array<Metric> $metrics
     *
     * @return array<string>
     */
    private function buildMetricLines(array $metrics): array
    {
        $maxNameLength = 0;
        $maxValueLength = 0;

        foreach ($metrics as $metric) {
            $maxNameLength = max($maxNameLength, mb_strlen($metric->name));
            $maxValueLength = max($maxValueLength, mb_strlen($this->formatMetricValue($metric)));
        }

        $lines = [];

        foreach ($metrics as $metric) {
            $icon = $this->metricTypeIcon($metric->type);
            $name = $this->output->pad($metric->name, $maxNameLength);
            $value = $this->output->pad($this->formatMetricValue($metric), $maxValueLength);
            $unit = $metric->unit !== null ? '[' . $metric->unit . ']' : '';

            $line = $this->output->blue($icon) . ' ';
            $line .= $name . '  ';
            $line .= $value . '  ';
            $line .= $this->output->dim($unit);

            if ($this->metricOptions->showDescription && $metric->description !== null) {
                $line .= '  ' . $this->output->dim('// ' . $metric->description);
            }

            if (!$metric->attributes->isEmpty()) {
                $attrParts = [];

                foreach ($metric->attributes->normalize() as $key => $attrValue) {
                    $attrParts[] =
                        $key . '=' . (is_scalar($attrValue) ? (string) $attrValue : (json_encode($attrValue) ?: ''));
                }
                $line .= '  ' . $this->output->dim('{' . implode(', ', $attrParts) . '}');
            }

            if ($this->metricOptions->showAggregationTemporality) {
                $line .= '  ' . $this->output->dim('[' . $metric->temporality->name . ']');
            }

            if ($this->metricOptions->showStartTimestamp && $metric->startTimestamp !== null) {
                $line .= '  ' . $this->output->dim('start:' . $metric->startTimestamp->format('H:i:s.u'));
            }

            $this->appendMetricExemplarInfo($line, $metric);
            $lines[] = $line;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildMetricResourceLines(TelemetryResource $resource): array
    {
        if ($resource->isEmpty()) {
            return [];
        }

        if (!$this->metricOptions->showResourceAttributes) {
            return $this->buildShortResourceLines($resource);
        }

        return $this->buildFullResourceLines($resource);
    }

    private function buildMetricScopeLine(Metric $metric): ?string
    {
        if (!$this->metricOptions->showInstrumentationScope) {
            return null;
        }

        $scope = $metric->scope;

        return 'Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version);
    }

    /**
     * @return array<string>
     */
    private function buildShortResourceLines(TelemetryResource $resource): array
    {
        $parts = [];
        $serviceName = $resource->get('service.name');

        if (is_string($serviceName)) {
            $parts[] = $serviceName;
        }

        $serviceVersion = $resource->get('service.version');

        if (is_string($serviceVersion)) {
            $parts[] = 'v' . $serviceVersion;
        }

        if (count($parts) === 0) {
            return [];
        }

        return ['Resource: ' . $this->output->dim(implode(' ', $parts))];
    }

    /**
     * @return array<string>
     */
    private function buildSpanAttributeLines(Span $span): array
    {
        $attributes = $span->attributes();

        if (count($attributes) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Attributes:');

        $maxKeyLength = 0;

        foreach (array_keys($attributes) as $key) {
            $maxKeyLength = max($maxKeyLength, mb_strlen($key));
        }

        foreach ($attributes as $key => $value) {
            $keyStr = $this->output->pad($key, $maxKeyLength);
            $valueStr = $this->output->formatValue($value);
            $lines[] = '  ' . $this->output->cyan($keyStr) . ' = ' . $valueStr;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildSpanDroppedCountsLines(Span $span): array
    {
        if (!$this->spanOptions->showDroppedCounts) {
            return [];
        }

        $droppedAttrs = $span->droppedAttributeCount();
        $droppedEvents = $span->droppedEventsCount();
        $droppedLinks = $span->droppedLinksCount();

        if ($droppedAttrs === 0 && $droppedEvents === 0 && $droppedLinks === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Dropped:');

        if ($droppedAttrs > 0) {
            $lines[] = '  ' . $this->output->yellow('Attributes: ' . $droppedAttrs);
        }

        if ($droppedEvents > 0) {
            $lines[] = '  ' . $this->output->yellow('Events: ' . $droppedEvents);
        }

        if ($droppedLinks > 0) {
            $lines[] = '  ' . $this->output->yellow('Links: ' . $droppedLinks);
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildSpanEventLines(Span $span): array
    {
        $events = $span->events();

        if (count($events) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Events (' . count($events) . '):');

        foreach ($events as $event) {
            $timestamp = $event->timestamp()->format('H:i:s.u');
            $attrs = $event->attributes();
            $attrStr = count($attrs) > 0 ? ' ' . $this->output->dim($this->output->formatValue($attrs)) : '';
            $lines[] = '  ' . $this->output->gray($timestamp) . ' ' . $event->name() . $attrStr;
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildSpanHeaderLines(Span $span): array
    {
        $context = $span->context();
        $lines = [];

        $lines[] = 'SPAN: ' . $span->name();

        $traceInfo = 'Trace: ' . $this->output->dim($context->traceId->toHex());
        $traceInfo .= '  Span: ' . $this->output->dim($context->spanId->toHex());

        if ($context->parentSpanId !== null) {
            $traceInfo .= '  Parent: ' . $this->output->dim($context->parentSpanId->toHex());
        }

        $lines[] = $traceInfo;

        $kindStr = $this->output->cyan(strtoupper($span->kind()->value));
        $statusStr = $this->formatSpanStatusIcon($span);
        $durationStr = $this->output->formatDuration($span->duration());

        $details = $this->output->pad('Kind: ' . $kindStr, 22);
        $details .= 'Status: ' . $statusStr . '  ';
        $details .= 'Duration: ' . $durationStr;
        $lines[] = $details;

        $lines[] = 'Start: ' . $this->output->formatTimestamp($span->startTime());

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildSpanLinkLines(Span $span): array
    {
        if (!$this->spanOptions->showLinks) {
            return [];
        }

        $links = $span->links();

        if (count($links) === 0) {
            return [];
        }

        $lines = [];
        $lines[] = $this->output->bold('Links (' . count($links) . '):');

        foreach ($links as $link) {
            $traceId = $link->context->traceId->toHex();
            $spanId = $link->context->spanId->toHex();
            $lines[] =
                '  -> '
                . $this->output->dim(
                    'trace:' . mb_substr($traceId, 0, 12) . '... span:' . mb_substr($spanId, 0, 12) . '...',
                );
        }

        return $lines;
    }

    /**
     * @return array<string>
     */
    private function buildSpanResourceLines(TelemetryResource $resource): array
    {
        if ($resource->isEmpty()) {
            return [];
        }

        if (!$this->spanOptions->showResourceAttributes) {
            return $this->buildShortResourceLines($resource);
        }

        return $this->buildFullResourceLines($resource);
    }

    /**
     * @return array<string>
     */
    private function buildSpanScopeLines(Span $span): array
    {
        if (!$this->spanOptions->showInstrumentationScope) {
            return [];
        }

        $scope = $span->scope();

        return ['Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version)];
    }

    /**
     * @param array<string> $lines
     */
    private function calculateLineWidth(array $lines, int $minWidth): int
    {
        $maxLength = 0;

        foreach ($lines as $line) {
            $visibleLength = mb_strlen($this->output->stripColors($line));
            $maxLength = max($maxLength, $visibleLength);
        }

        return max($minWidth, $maxLength + 4);
    }

    /**
     * @param array<array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int}> $records
     */
    private function calculateLogBodyWidth(array $records): int
    {
        $maxLength = 0;

        foreach ($records as $record) {
            $maxLength = max($maxLength, mb_strlen($record['body']));
        }

        if ($this->maxLogBodyLength !== null) {
            return min($maxLength, $this->maxLogBodyLength);
        }

        return $maxLength;
    }

    private function calculateLogTotalWidth(int $bodyWidth, bool $hasTrace): int
    {
        $width = 4;
        $width += self::LOG_TIMESTAMP_WIDTH;
        $width += 3;
        $width += self::LOG_SEVERITY_WIDTH;
        $width += 3;
        $width += $bodyWidth;

        if ($hasTrace) {
            $width += 3;
            $width += self::LOG_TRACE_WIDTH;
        }

        return max(self::LOG_MIN_WIDTH, $width);
    }

    private function colorBySeverity(string $text, Severity $severity): string
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
     * @param array<LogEntry> $entries
     */
    private function exportLogs(array $entries): bool
    {
        if (count($entries) === 0) {
            return true;
        }

        $formattedRecords = $this->formatLogEntries($entries);
        $bodyWidth = $this->calculateLogBodyWidth($formattedRecords);
        $hasTrace = $this->hasAnySpanContext($entries);
        $resourceLines = $this->buildLogResourceLines($entries[0]->resource);
        $scopeLine = $this->buildLogScopeLine($entries[0]);
        $width = $this->calculateLogTotalWidth($bodyWidth, $hasTrace);

        foreach ($resourceLines as $resourceLine) {
            $resourceWidth = mb_strlen($this->output->stripColors($resourceLine)) + 4;
            $width = max($width, $resourceWidth);
        }

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold('LOGS'), $width) . PHP_EOL;

        if (count($resourceLines) > 0) {
            foreach ($resourceLines as $resourceLine) {
                $buffer .= $this->output->row($resourceLine, $width) . PHP_EOL;
            }
        }

        if ($scopeLine !== null) {
            $buffer .= $this->output->row($scopeLine, $width) . PHP_EOL;
        }

        $buffer .= $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->buildLogHeaderLine($bodyWidth, $hasTrace), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        foreach ($formattedRecords as $record) {
            $buffer .= $this->output->row($this->buildLogLine($record, $bodyWidth, $hasTrace), $width) . PHP_EOL;
        }

        $buffer .= $this->output->border($width);

        $this->output->write($buffer);

        return true;
    }

    /**
     * @param array<Metric> $metrics
     */
    private function exportMetrics(array $metrics): bool
    {
        if (count($metrics) === 0) {
            return true;
        }

        $lines = $this->buildMetricLines($metrics);
        $resourceLines = $this->buildMetricResourceLines($metrics[0]->resource);
        $scopeLine = $this->buildMetricScopeLine($metrics[0]);

        $allLines = array_merge($resourceLines, $scopeLine !== null ? [$scopeLine] : [], $lines);
        $maxLength = mb_strlen('METRICS');

        foreach ($allLines as $line) {
            $maxLength = max($maxLength, mb_strlen($this->output->stripColors($line)));
        }

        $width = max(self::METRIC_MIN_WIDTH, $maxLength + 4);

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold('METRICS'), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        foreach ($resourceLines as $resourceLine) {
            $buffer .= $this->output->row($resourceLine, $width) . PHP_EOL;
        }

        if ($scopeLine !== null) {
            $buffer .= $this->output->row($scopeLine, $width) . PHP_EOL;
        }

        if (count($resourceLines) > 0 || $scopeLine !== null) {
            $buffer .= $this->output->border($width) . PHP_EOL;
        }

        foreach ($lines as $line) {
            $buffer .= $this->output->row($line, $width) . PHP_EOL;
        }

        $buffer .= $this->output->border($width);

        $this->output->write($buffer);

        return true;
    }

    /**
     * @param array<Span> $spans
     */
    private function exportSpans(array $spans): bool
    {
        foreach ($spans as $span) {
            $this->printSpan($span);
        }

        return true;
    }

    /**
     * @param array<string, array<array-key, mixed>|bool|float|int|string> $attributes
     */
    private function formatLogAttributes(array $attributes): string
    {
        if (count($attributes) === 0) {
            return '';
        }

        $parts = [];

        foreach ($attributes as $key => $value) {
            $parts[] = $key . ': ' . $this->output->formatValue($value);
        }

        return '{' . implode(', ', $parts) . '}';
    }

    /**
     * @param array<LogEntry> $entries
     *
     * @return array<array{timestamp: string, severity: string, severityRaw: Severity, body: string, trace: string, droppedAttributeCount: int}>
     */
    private function formatLogEntries(array $entries): array
    {
        $formatted = [];

        foreach ($entries as $entry) {
            $body = $entry->record->body;
            $attrs = $this->formatLogAttributes($entry->record->attributes->normalize());

            if ($attrs !== '') {
                $body .= ' ' . $attrs;
            }

            $trace = '';

            if ($entry->spanContext !== null) {
                $trace =
                    mb_substr($entry->spanContext->traceId->toHex(), 0, 8)
                    . '/'
                    . mb_substr($entry->spanContext->spanId->toHex(), 0, 8);
            }

            $timestamp = $entry->timestamp->format('Y-m-d H:i:s.u');

            if ($this->logOptions->showObservedTimestamp && $entry->record->observedTimestamp !== null) {
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

    private function formatMetricValue(Metric $metric): string
    {
        $value = $metric->value;

        if (is_float($value)) {
            if (abs($value) >= 1000000) {
                return sprintf('%.2fM', $value / 1000000);
            }

            if (abs($value) >= 1000) {
                return sprintf('%.2fK', $value / 1000);
            }

            return sprintf('%.2f', $value);
        }

        if ($value >= 1000000) {
            return sprintf('%.2fM', $value / 1000000);
        }

        if ($value >= 1000) {
            return number_format($value);
        }

        return (string) $value;
    }

    private function formatSpanStatusIcon(Span $span): string
    {
        $status = $span->status();

        if ($status === null) {
            return $this->output->yellow('UNSET');
        }

        $statusText = match ($status->code) {
            SpanStatusCode::OK => $this->output->green('OK'),
            SpanStatusCode::ERROR => $this->output->red('ERROR'),
            SpanStatusCode::UNSET => $this->output->yellow('UNSET'),
        };

        if (
            $this->spanOptions->showStatusDescription
            && $status->code === SpanStatusCode::ERROR
            && $status->description !== null
        ) {
            $statusText .= ' ' . $this->output->dim('(' . $status->description . ')');
        }

        return $statusText;
    }

    /**
     * @param array<LogEntry> $entries
     */
    private function hasAnySpanContext(array $entries): bool
    {
        foreach ($entries as $entry) {
            if ($entry->spanContext !== null) {
                return true;
            }
        }

        return false;
    }

    private function metricTypeIcon(MetricType $type): string
    {
        return match ($type) {
            MetricType::COUNTER => '^',
            MetricType::UP_DOWN_COUNTER => '~',
            MetricType::GAUGE => 'o',
            MetricType::HISTOGRAM => '#',
        };
    }

    private function printSpan(Span $span): void
    {
        $headerLines = $this->buildSpanHeaderLines($span);
        $resourceLines = $this->buildSpanResourceLines($span->resource());
        $scopeLines = $this->buildSpanScopeLines($span);
        $attributeLines = $this->buildSpanAttributeLines($span);
        $eventLines = $this->buildSpanEventLines($span);
        $linkLines = $this->buildSpanLinkLines($span);
        $droppedLines = $this->buildSpanDroppedCountsLines($span);

        $allLines = array_merge(
            $headerLines,
            $resourceLines,
            $scopeLines,
            $attributeLines,
            $eventLines,
            $linkLines,
            $droppedLines,
        );
        $width = $this->calculateLineWidth($allLines, self::SPAN_MIN_WIDTH);

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold($headerLines[0]), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        for ($i = 1; $i < count($headerLines); $i++) {
            $buffer .= $this->output->row($headerLines[$i], $width) . PHP_EOL;
        }

        if (count($resourceLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($resourceLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (count($scopeLines) > 0) {
            foreach ($scopeLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (count($attributeLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($attributeLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (count($eventLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($eventLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (count($linkLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($linkLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        if (count($droppedLines) > 0) {
            $buffer .= $this->output->border($width) . PHP_EOL;

            foreach ($droppedLines as $line) {
                $buffer .= $this->output->row($line, $width) . PHP_EOL;
            }
        }

        $buffer .= $this->output->border($width);

        $this->output->write($buffer);
    }
}
