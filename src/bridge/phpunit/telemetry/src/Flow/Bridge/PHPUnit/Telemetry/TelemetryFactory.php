<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_options, otlp_curl_transport, otlp_grpc_transport, otlp_json_serializer, otlp_metric_exporter, otlp_protobuf_serializer, otlp_span_exporter};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, meter_provider, pass_through_log_processor, pass_through_metric_processor, pass_through_span_processor, resource, resource_detector, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};

use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Transport\Transport;

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

        $transport = self::buildTransport($config->transport);

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

    private static function buildCurlTransport(CurlTransportConfig $config) : Transport
    {
        $options = otlp_curl_options()
            ->withTimeout($config->timeout)
            ->withConnectTimeout($config->connectTimeout)
            ->withCompression($config->compression)
            ->withFollowRedirects($config->followRedirects, $config->maxRedirects)
            ->withSslVerification($config->sslVerifyPeer, $config->sslVerifyHost);

        foreach ($config->headers as $name => $value) {
            $options = $options->withHeader($name, $value);
        }

        if ($config->proxy !== null) {
            $options = $options->withProxy($config->proxy);
        }

        if ($config->sslCertPath !== null) {
            $options = $options->withSslCertificate($config->sslCertPath, $config->sslKeyPath);
        }

        if ($config->caInfoPath !== null) {
            $options = $options->withCaInfo($config->caInfoPath);
        }

        $serializer = match ($config->serializer) {
            SerializerType::JSON => otlp_json_serializer(),
            SerializerType::PROTOBUF => otlp_protobuf_serializer(),
        };

        return otlp_curl_transport($config->endpoint, $serializer, $options);
    }

    private static function buildGrpcTransport(GrpcTransportConfig $config) : Transport
    {
        return otlp_grpc_transport(
            endpoint: $config->endpoint,
            serializer: otlp_protobuf_serializer(),
            headers: $config->headers,
            insecure: $config->insecure,
        );
    }

    private static function buildTransport(CurlTransportConfig|GrpcTransportConfig $config) : Transport
    {
        return match (true) {
            $config instanceof CurlTransportConfig => self::buildCurlTransport($config),
            $config instanceof GrpcTransportConfig => self::buildGrpcTransport($config),
        };
    }
}
