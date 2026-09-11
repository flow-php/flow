<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Profiler;

use DateTimeImmutable;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
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
 * @type SpanRow = array{name: string, kind: string, durationMs: null|float, startMs: float, offsetMs: float, startTime: string, scope: string, spanId: string, parentSpanId: null|string, depth: int, statusCode: int, statusDescription: null|string, attributes: array<string, mixed>, eventCount: int}
 * @type MetricRow = array{name: string, type: string, value: float|int, unit: null|string, scope: string, attributes: array<string, mixed>}
 * @type LogRow = array{severity: string, severityCode: int, message: string, scope: string, time: string, attributes: \Symfony\Component\VarDumper\Cloner\Data, hasAttributes: bool}
 * @type ScopeRow = array{name: string, version: string, attributes: array<string, mixed>}
 * @type InstrumentRow = array{type: string, name: string, version: string, attributes: array<string, mixed>}
 */
final class FlowTelemetryDataCollector extends DataCollector implements LateDataCollectorInterface
{
    private ?Span $requestSpan = null;

    private ?DateTimeImmutable $requestSpanEnd = null;

    /**
     * @param list<InstrumentRow> $configuredInstruments
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly MemoryExporter $exporter,
        private readonly array $configuredInstruments = [],
    ) {}

    public function collect(Request $request, Response $response, ?Throwable $exception = null): void
    {
        // @mago-expect analysis:mixed-assignment
        $span = $request->attributes->get(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE);

        // The HTTP server span completes on kernel.terminate, after the profiler saves the profile, so it
        // never reaches the store. Capture it here while still open and snapshot the response moment as its
        // display end; the real span keeps ending (and exporting) on terminate untouched.
        if ($span instanceof Span) {
            $this->requestSpan = $span;
            $this->requestSpanEnd = new DateTimeImmutable();
        }
    }

    public function lateCollect(): void
    {
        // The bundle flushes telemetry on kernel.terminate (priority -20000), long after the
        // profiler saves the profile.
        $this->telemetry->flush();

        $spans = $this->normalizeSpans($this->exporter->spans(), $this->requestSpan, $this->requestSpanEnd);

        $timelineDurationMs = 0.0;

        foreach ($spans as $span) {
            $timelineDurationMs = max($timelineDurationMs, $span['offsetMs'] + ($span['durationMs'] ?? 0.0));
        }

        $this->data = [
            'spans' => $spans,
            'metrics' => $this->normalizeMetrics($this->exporter->metrics()),
            'logs' => $this->normalizeLogs($this->exporter->logs()),
            'timelineDurationMs' => $timelineDurationMs,
            'resourceAttributes' => $this->resourceAttributes(),
            'scopes' => $this->normalizeScopes(),
            'configuredInstruments' => $this->configuredInstruments,
        ];
    }

    public function reset(): void
    {
        $this->data = [];
        $this->requestSpan = null;
        $this->requestSpanEnd = null;
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

    /**
     * @return array<string, mixed>
     */
    public function getResourceAttributes(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['resourceAttributes'] ?? [];
    }

    /**
     * @return list<ScopeRow>
     */
    public function getScopes(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['scopes'] ?? [];
    }

    /**
     * Named tracers/meters/loggers from configuration, shown regardless of whether they emitted a signal.
     *
     * @return list<InstrumentRow>
     */
    public function getConfiguredInstruments(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['configuredInstruments'] ?? [];
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
    private function normalizeSpans(array $spans, ?Span $inFlight = null, ?DateTimeImmutable $inFlightEnd = null): array
    {
        if ($inFlight !== null && $inFlightEnd !== null && !$this->isCaptured($inFlight, $spans)) {
            $spans[] = $inFlight;
        }

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
            $startMs = ($start->getTimestamp() * 1000.0) + ((int) $start->format('u') / 1000.0);

            $durationMs =
                $span === $inFlight && $inFlightEnd !== null
                    ? ($inFlightEnd->getTimestamp() * 1000.0) + ((int) $inFlightEnd->format('u') / 1000.0) - $startMs
                    : $span->duration();

            $rows[] = [
                'name' => $span->name(),
                'kind' => $span->kind()->value,
                'durationMs' => $durationMs,
                'startMs' => $startMs,
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
     * @param array<Span> $spans
     */
    private function isCaptured(Span $span, array $spans): bool
    {
        $spanId = $span->context()->normalize()['spanId']['hex'];

        foreach ($spans as $captured) {
            if ($captured->context()->normalize()['spanId']['hex'] === $spanId) {
                return true;
            }
        }

        return false;
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

    /**
     * The resource is shared by every signal from the provider, so the first available one describes the request.
     *
     * @return array<string, mixed>
     */
    private function resourceAttributes(): array
    {
        foreach ($this->exporter->spans() as $span) {
            return $span->resource()->normalize()['attributes'];
        }

        foreach ($this->exporter->metrics() as $metric) {
            return $metric->resource->normalize()['attributes'];
        }

        foreach ($this->exporter->logs() as $log) {
            return $log->resource->normalize()['attributes'];
        }

        return [];
    }

    /**
     * Distinct instrumentation scopes across all signals, identified by name + version.
     *
     * @return list<ScopeRow>
     */
    private function normalizeScopes(): array
    {
        $instrumentationScopes = [];

        foreach ($this->exporter->spans() as $span) {
            $instrumentationScopes[] = $span->scope();
        }

        foreach ($this->exporter->metrics() as $metric) {
            $instrumentationScopes[] = $metric->scope;
        }

        foreach ($this->exporter->logs() as $log) {
            $instrumentationScopes[] = $log->scope;
        }

        $scopes = [];

        foreach ($instrumentationScopes as $scope) {
            $scopes[$scope->name . '@' . $scope->version] ??= [
                'name' => $scope->name,
                'version' => $scope->version,
                'attributes' => $scope->attributes->normalize(),
            ];
        }

        usort($scopes, static fn(array $a, array $b): int => $a['name'] <=> $b['name']);

        return $scopes;
    }
}
