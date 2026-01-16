<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricType};
use Flow\Telemetry\Resource as TelemetryResource;
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Console exporter for metrics with ASCII table visualization.
 *
 * Outputs metrics to the console in a human-readable format with optional ANSI colors.
 * Table width is calculated dynamically based on content.
 */
final readonly class ConsoleMetricExporter implements MetricExporter
{
    private const int MIN_WIDTH = 40;

    private ConsoleOutput $output;

    /**
     * @param bool $colors Whether to use ANSI colors
     * @param null|resource $outputStream Output stream (default: STDOUT)
     */
    public function __construct(
        bool $colors = true,
        mixed $outputStream = null,
    ) {
        /** @var null|resource $outputStream */
        $this->output = new ConsoleOutput($colors, $outputStream);
    }

    /**
     * @param array<Metric> $metrics
     */
    public function export(array $metrics) : bool
    {
        if (\count($metrics) === 0) {
            return true;
        }

        $lines = $this->buildMetricLines($metrics);
        $resourceLine = $this->buildResourceLine($metrics[0]->resource);

        if ($resourceLine !== null) {
            \array_unshift($lines, $resourceLine);
        }

        $width = $this->calculateWidth($lines);

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold('METRICS'), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        foreach ($lines as $line) {
            $buffer .= $this->output->row($line, $width) . PHP_EOL;
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

    /**
     * @param array<Metric> $metrics
     *
     * @return array<string>
     */
    private function buildMetricLines(array $metrics) : array
    {
        $maxNameLength = 0;
        $maxValueLength = 0;

        foreach ($metrics as $metric) {
            $maxNameLength = \max($maxNameLength, \mb_strlen($metric->name));
            $maxValueLength = \max($maxValueLength, \mb_strlen($this->formatValue($metric)));
        }

        $lines = [];

        foreach ($metrics as $metric) {
            $icon = $this->typeIcon($metric->type);
            $name = $this->output->pad($metric->name, $maxNameLength);
            $value = $this->output->pad($this->formatValue($metric), $maxValueLength);
            $unit = $metric->unit !== null ? '[' . $metric->unit . ']' : '';

            $line = $this->output->blue($icon) . ' ';
            $line .= $name . '  ';
            $line .= $value . '  ';
            $line .= $this->output->dim($unit);

            if (\count($metric->exemplars) > 0) {
                $exemplar = $metric->exemplars[0];
                $traceIdShort = \substr($exemplar->traceId->toHex(), 0, 8);
                $spanIdShort = \substr($exemplar->spanId->toHex(), 0, 8);
                $line .= '  ' . $this->output->dim('[trace:' . $traceIdShort . '.. span:' . $spanIdShort . '..]');
            }

            $lines[] = $line;
        }

        return $lines;
    }

    private function buildResourceLine(TelemetryResource $resource) : ?string
    {
        if ($resource->isEmpty()) {
            return null;
        }

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
            return null;
        }

        return 'Resource: ' . $this->output->dim(\implode(' ', $parts));
    }

    /**
     * @param array<string> $lines
     */
    private function calculateWidth(array $lines) : int
    {
        $maxLength = \mb_strlen('METRICS');

        foreach ($lines as $line) {
            $visibleLength = \mb_strlen($this->output->stripColors($line));
            $maxLength = \max($maxLength, $visibleLength);
        }

        return \max(self::MIN_WIDTH, $maxLength + 4);
    }

    private function formatValue(Metric $metric) : string
    {
        $value = $metric->value;

        if (\is_float($value)) {
            if (\abs($value) >= 1000000) {
                return \sprintf('%.2fM', $value / 1000000);
            }

            if (\abs($value) >= 1000) {
                return \sprintf('%.2fK', $value / 1000);
            }

            return \sprintf('%.2f', $value);
        }

        if ($value >= 1000000) {
            return \sprintf('%.2fM', $value / 1000000);
        }

        if ($value >= 1000) {
            return \number_format($value);
        }

        return (string) $value;
    }

    private function typeIcon(MetricType $type) : string
    {
        return match ($type) {
            MetricType::COUNTER => '^',
            MetricType::UP_DOWN_COUNTER => '~',
            MetricType::GAUGE => 'o',
            MetricType::HISTOGRAM => '#',
        };
    }
}
