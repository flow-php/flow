<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_json_serializer, otlp_metric_exporter, otlp_span_exporter};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, meter_provider, pass_through_log_processor, pass_through_metric_processor, pass_through_span_processor, resource, resource_detector, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;

final class TelemetryFactory
{
    public static function create(Configuration $config) : Telemetry
    {
        $telemetryResource = resource_detector()->detect()->merge(
            resource([
                'service.name' => $config->serviceName,
                'telemetry.sdk.name' => 'flow-php-phpunit-telemetry',
                'telemetry.sdk.language' => 'php',
            ])
        );

        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $transport = otlp_curl_transport(
            endpoint: $config->otelCollectorUrl,
            serializer: otlp_json_serializer(),
        );

        $spanProcessor = $config->emitTraces
            ? pass_through_span_processor(otlp_span_exporter($transport))
            : pass_through_span_processor(void_span_exporter());

        $metricProcessor = $config->emitMetrics
            ? pass_through_metric_processor(otlp_metric_exporter($transport))
            : pass_through_metric_processor(void_metric_exporter());

        $logProcessor = pass_through_log_processor(void_log_exporter());

        return telemetry(
            $telemetryResource,
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider($metricProcessor, $clock),
            logger_provider($logProcessor, $clock, $contextStorage),
        );
    }
}
