<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;

use function max;
use function preg_match_all;
use function preg_quote;
use function usleep;

/**
 * Helper class to query OTEL Collector's Prometheus metrics endpoint.
 *
 * Used to verify that telemetry data flows through the collector.
 * Queries `otelcol_exporter_sent_*_total` metrics which indicate data was
 * successfully exported (more reliable than receiver metrics in v0.115+).
 * The collector exposes internal metrics at port 8888.
 */
final readonly class CollectorMetrics
{
    public function __construct(
        private ClientInterface $httpClient,
        private RequestFactoryInterface $requestFactory,
        private string $metricsEndpoint = 'http://localhost:8888/metrics',
    ) {}

    public function getAcceptedLogRecords(): int
    {
        return $this->getMetricValue('otelcol_exporter_sent_log_records_total');
    }

    public function getAcceptedMetricPoints(): int
    {
        return $this->getMetricValue('otelcol_exporter_sent_metric_points_total');
    }

    public function getAcceptedSpans(): int
    {
        return $this->getMetricValue('otelcol_exporter_sent_spans_total');
    }

    /**
     * Wait until accepted log records count exceeds the given threshold.
     *
     * @param int $threshold The minimum expected count
     * @param int $timeoutMs Maximum wait time in milliseconds (default: 500ms)
     * @param int $pollIntervalMs Poll interval in milliseconds (default: 10ms)
     */
    public function waitForLogRecords(int $threshold, int $timeoutMs = 5000, int $pollIntervalMs = 10): int
    {
        return $this->waitForMetric('otelcol_exporter_sent_log_records_total', $threshold, $timeoutMs, $pollIntervalMs);
    }

    /**
     * Wait until accepted metric points count exceeds the given threshold.
     *
     * @param int $threshold The minimum expected count
     * @param int $timeoutMs Maximum wait time in milliseconds (default: 500ms)
     * @param int $pollIntervalMs Poll interval in milliseconds (default: 10ms)
     */
    public function waitForMetricPoints(int $threshold, int $timeoutMs = 5000, int $pollIntervalMs = 10): int
    {
        return $this->waitForMetric(
            'otelcol_exporter_sent_metric_points_total',
            $threshold,
            $timeoutMs,
            $pollIntervalMs,
        );
    }

    /**
     * Wait until accepted spans count exceeds the given threshold.
     *
     * @param int $threshold The minimum expected count
     * @param int $timeoutMs Maximum wait time in milliseconds (default: 500ms)
     * @param int $pollIntervalMs Poll interval in milliseconds (default: 10ms)
     */
    public function waitForSpans(int $threshold, int $timeoutMs = 5000, int $pollIntervalMs = 10): int
    {
        return $this->waitForMetric('otelcol_exporter_sent_spans_total', $threshold, $timeoutMs, $pollIntervalMs);
    }

    private function getMetricValue(string $metricName): int
    {
        $request = $this->requestFactory->createRequest('GET', $this->metricsEndpoint);
        $response = $this->httpClient->sendRequest($request);
        $body = (string) $response->getBody();

        $total = 0;
        $matches = [];

        if (preg_match_all('/' . preg_quote($metricName, '/') . '\{[^}]*\}\s+(\d+)/', $body, $matches)) {
            foreach ($matches[1] as $value) {
                $total += (int) $value;
            }
        }

        return $total;
    }

    private function waitForMetric(string $metricName, int $threshold, int $timeoutMs, int $pollIntervalMs): int
    {
        $startTime = HighResolutionTime::now();
        $timeoutSeconds = $timeoutMs / 1000;

        while (true) {
            $value = $this->getMetricValue($metricName);

            if ($value > $threshold) {
                return $value;
            }

            $elapsedSeconds = $startTime->diff(HighResolutionTime::now())->toSeconds();

            if ($elapsedSeconds >= $timeoutSeconds) {
                return $value;
            }

            usleep(max(0, $pollIntervalMs * 1000));
        }
    }
}
