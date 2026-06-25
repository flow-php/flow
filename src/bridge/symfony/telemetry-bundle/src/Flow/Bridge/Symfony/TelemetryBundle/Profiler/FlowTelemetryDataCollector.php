<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Profiler;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\DataCollector\DataCollector;
use Symfony\Component\HttpKernel\DataCollector\LateDataCollectorInterface;
use Throwable;

use function array_key_exists;
use function count;
use function is_float;
use function max;
use function usort;

/**
 * @phpstan-type SpanRow array{name: string, kind: string, durationMs: null|float, startMs: float, offsetMs: float, startTime: string, scope: string, spanId: string, parentSpanId: null|string, depth: int, statusCode: int, statusDescription: null|string, attributes: array<string, mixed>, eventCount: int}
 * @phpstan-type MetricRow array{name: string, type: string, value: float|int, unit: null|string, scope: string, attributes: array<string, mixed>}
 * @phpstan-type LogRow array{severity: string, severityCode: int, message: string, scope: string, time: string, attributes: \Symfony\Component\VarDumper\Cloner\Data, hasAttributes: bool}
 */
final class FlowTelemetryDataCollector extends DataCollector implements LateDataCollectorInterface
{
    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly MemoryExporter $exporter,
    ) {}

    public function collect(Request $request, Response $response, ?Throwable $exception = null): void {}

    public function lateCollect(): void
    {
        // The bundle flushes telemetry on kernel.terminate (priority -20000), long after the
        // profiler saves the profile.
        $this->telemetry->flush();

        $spans = $this->normalizeSpans($this->exporter->spans());

        $timelineDurationMs = 0.0;

        foreach ($spans as $span) {
            $timelineDurationMs = max($timelineDurationMs, $span['offsetMs'] + ($span['durationMs'] ?? 0.0));
        }

        $this->data = [
            'spans' => $spans,
            'metrics' => $this->normalizeMetrics($this->exporter->metrics()),
            'logs' => $this->normalizeLogs($this->exporter->logs()),
            'timelineDurationMs' => $timelineDurationMs,
        ];
    }

    public function reset(): void
    {
        $this->data = [];
        $this->exporter->reset();
    }

    public function getName(): string
    {
        return 'flow_telemetry';
    }

    /**
     * @return list<SpanRow>
     */
    public function getSpans(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['spans'] ?? [];
    }

    /**
     * @return list<MetricRow>
     */
    public function getMetrics(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['metrics'] ?? [];
    }

    /**
     * @return list<LogRow>
     */
    public function getLogs(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['logs'] ?? [];
    }

    public function getSpanCount(): int
    {
        return count($this->getSpans());
    }

    public function getMetricCount(): int
    {
        return count($this->getMetrics());
    }

    public function getLogCount(): int
    {
        return count($this->getLogs());
    }

    public function getSignalCount(): int
    {
        return $this->getSpanCount() + $this->getMetricCount() + $this->getLogCount();
    }

    public function getTimelineDurationMs(): float
    {
        // @mago-expect analysis:mixed-assignment
        $value = $this->data['timelineDurationMs'] ?? 0.0;

        return is_float($value) ? $value : 0.0;
    }

    /**
     * @param array<Span> $spans
     *
     * @return list<SpanRow>
     */
    private function normalizeSpans(array $spans): array
    {
        $parents = [];

        foreach ($spans as $span) {
            $context = $span->context()->normalize();
            $parents[$context['spanId']['hex']] = $context['parentSpanId']['hex'] ?? null;
        }

        $rows = [];

        foreach ($spans as $span) {
            $context = $span->context()->normalize();
            $status = $span->status()?->normalize();
            $spanId = $context['spanId']['hex'];
            $start = $span->startTime();

            $rows[] = [
                'name' => $span->name(),
                'kind' => $span->kind()->value,
                'durationMs' => $span->duration(),
                'startMs' => ($start->getTimestamp() * 1000.0) + ((int) $start->format('u') / 1000.0),
                'offsetMs' => 0.0,
                'startTime' => $start->format('H:i:s.v'),
                'scope' => $span->scope()->name,
                'spanId' => $spanId,
                'parentSpanId' => $parents[$spanId],
                'depth' => $this->depthOf($spanId, $parents),
                'statusCode' => $status['code'] ?? 0,
                'statusDescription' => $status['description'] ?? null,
                'attributes' => $span->attributesObject()->normalize(),
                'eventCount' => count($span->events()),
            ];
        }

        usort($rows, static fn(array $a, array $b): int => $a['startMs'] <=> $b['startMs']);

        // Offsets are relative to the earliest span, which anchors the waterfall.
        if ($rows !== []) {
            $base = $rows[0]['startMs'];

            foreach ($rows as $index => $row) {
                $rows[$index]['offsetMs'] = $row['startMs'] - $base;
            }
        }

        return $rows;
    }

    /**
     * @param array<string, null|string> $parents
     */
    private function depthOf(string $spanId, array $parents): int
    {
        $depth = 0;
        $current = $parents[$spanId] ?? null;

        while ($current !== null && array_key_exists($current, $parents)) {
            $depth++;
            $current = $parents[$current];
        }

        return $depth;
    }

    /**
     * @param array<Metric> $metrics
     *
     * @return list<MetricRow>
     */
    private function normalizeMetrics(array $metrics): array
    {
        $rows = [];

        foreach ($metrics as $metric) {
            $rows[] = [
                'name' => $metric->name,
                'type' => $metric->type->value,
                'value' => $metric->value,
                'unit' => $metric->unit,
                'scope' => $metric->scope->name,
                'attributes' => $metric->attributes->normalize(),
            ];
        }

        return $rows;
    }

    /**
     * @param array<LogEntry> $logs
     *
     * @return list<LogRow>
     */
    private function normalizeLogs(array $logs): array
    {
        $rows = [];

        foreach ($logs as $log) {
            $normalized = $log->normalize();
            $severity = $normalized['record']['severity'];
            $attributes = $normalized['record']['attributes'];

            $rows[] = [
                'severity' => Severity::tryFrom($severity)?->name() ?? 'unknown',
                'severityCode' => $severity,
                'message' => $normalized['record']['body'],
                'scope' => $normalized['scope']['name'],
                'time' => $normalized['timestamp'],
                'attributes' => $this->cloneVar($attributes),
                'hasAttributes' => $attributes !== [],
            ];
        }

        return $rows;
    }
}
