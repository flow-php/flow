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
        private ConsoleMetricOptions $options = new ConsoleMetricOptions(),
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
        $resourceLines = $this->buildResourceLines($metrics[0]->resource);
        $scopeLine = $this->buildScopeLine($metrics[0]);

        $allLines = \array_merge($resourceLines, $scopeLine !== null ? [$scopeLine] : [], $lines);
        $width = $this->calculateWidth($allLines);

        $buffer = $this->output->border($width) . PHP_EOL;
        $buffer .= $this->output->row($this->output->bold('METRICS'), $width) . PHP_EOL;
        $buffer .= $this->output->border($width) . PHP_EOL;

        foreach ($resourceLines as $resourceLine) {
            $buffer .= $this->output->row($resourceLine, $width) . PHP_EOL;
        }

        if ($scopeLine !== null) {
            $buffer .= $this->output->row($scopeLine, $width) . PHP_EOL;
        }

        if (\count($resourceLines) > 0 || $scopeLine !== null) {
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
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }

    private function appendExemplarInfo(string &$line, Metric $metric) : void
    {
        if (\count($metric->exemplars) === 0) {
            return;
        }

        if ($this->options->showAllExemplars) {
            foreach ($metric->exemplars as $exemplar) {
                $traceIdShort = \substr($exemplar->traceId->toHex(), 0, 8);
                $spanIdShort = \substr($exemplar->spanId->toHex(), 0, 8);
                $line .= '  ' . $this->output->dim('[trace:' . $traceIdShort . '.. span:' . $spanIdShort . '..]');
            }
        } else {
            $exemplar = $metric->exemplars[0];
            $traceIdShort = \substr($exemplar->traceId->toHex(), 0, 8);
            $spanIdShort = \substr($exemplar->spanId->toHex(), 0, 8);
            $line .= '  ' . $this->output->dim('[trace:' . $traceIdShort . '.. span:' . $spanIdShort . '..]');
        }
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

            if ($this->options->showDescription && $metric->description !== null) {
                $line .= '  ' . $this->output->dim('// ' . $metric->description);
            }

            if (!$metric->attributes->isEmpty()) {
                $attrParts = [];

                foreach ($metric->attributes->normalize() as $key => $attrValue) {
                    $attrParts[] = $key . '=' . (\is_scalar($attrValue) ? (string) $attrValue : \json_encode($attrValue));
                }
                $line .= '  ' . $this->output->dim('{' . \implode(', ', $attrParts) . '}');
            }

            if ($this->options->showAggregationTemporality) {
                $line .= '  ' . $this->output->dim('[' . $metric->temporality->name . ']');
            }

            if ($this->options->showStartTimestamp && $metric->startTimestamp !== null) {
                $line .= '  ' . $this->output->dim('start:' . $metric->startTimestamp->format('H:i:s.u'));
            }

            $this->appendExemplarInfo($line, $metric);
            $lines[] = $line;
        }

        return $lines;
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

    private function buildScopeLine(Metric $metric) : ?string
    {
        if (!$this->options->showInstrumentationScope) {
            return null;
        }

        $scope = $metric->scope;

        return 'Scope: ' . $this->output->dim($scope->name . ' v' . $scope->version);
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
