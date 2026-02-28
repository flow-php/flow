<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use PHPUnit\Runner\Extension\ParameterCollection;

final readonly class Configuration
{
    public function __construct(
        public string $serviceName,
        public string $otelCollectorUrl,
        public bool $emitTraces,
        public bool $emitMetrics,
        public bool $emitTestSpans,
        public bool $emitTestCaseSpans,
    ) {
    }

    public static function fromParameters(ParameterCollection $parameters) : self
    {
        return new self(
            serviceName: $parameters->has('service_name')
                ? $parameters->get('service_name')
                : 'phpunit',
            otelCollectorUrl: $parameters->has('otel_collector_url')
                ? $parameters->get('otel_collector_url')
                : 'http://localhost:4318',
            emitTraces: !$parameters->has('emit_traces')
                || $parameters->get('emit_traces') === 'true',
            emitMetrics: !$parameters->has('emit_metrics')
                || $parameters->get('emit_metrics') === 'true',
            emitTestSpans: !$parameters->has('emit_test_spans')
                || $parameters->get('emit_test_spans') === 'true',
            emitTestCaseSpans: !$parameters->has('emit_test_case_spans')
                || $parameters->get('emit_test_case_spans') === 'true',
        );
    }
}
