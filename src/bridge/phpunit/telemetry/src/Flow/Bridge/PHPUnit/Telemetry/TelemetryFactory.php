<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Bridge\Telemetry\OTLP\Serializer\SerializerType;
use Flow\Bridge\Telemetry\OTLP\Transport\Transport;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\ErrorHandler\StreamHandler;
use Flow\Telemetry\ErrorHandler\SyslogHandler;
use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_options;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_transport;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_exporter;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_grpc_transport;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_json_serializer;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_protobuf_serializer;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_stream_transport;
use function Flow\Telemetry\DSL\batching_log_processor;
use function Flow\Telemetry\DSL\batching_metric_processor;
use function Flow\Telemetry\DSL\batching_span_processor;
use function Flow\Telemetry\DSL\composer_detector;
use function Flow\Telemetry\DSL\environment_detector;
use function Flow\Telemetry\DSL\git_detector;
use function Flow\Telemetry\DSL\host_detector;
use function Flow\Telemetry\DSL\logger_provider;
use function Flow\Telemetry\DSL\memory_context_storage;
use function Flow\Telemetry\DSL\meter_provider;
use function Flow\Telemetry\DSL\os_detector;
use function Flow\Telemetry\DSL\process_detector;
use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\resource_detector;
use function Flow\Telemetry\DSL\telemetry;
use function Flow\Telemetry\DSL\tracer_provider;
use function Flow\Telemetry\DSL\void_exporter;

final class TelemetryFactory
{
    public static function create(Configuration $config): Telemetry
    {
        $telemetryResource = resource_detector([
            os_detector(),
            host_detector(),
            process_detector(),
            composer_detector(),
            environment_detector(),
            git_detector(),
        ])
            ->detect()
            ->merge(resource([
                'service.name' => $config->serviceName,
                'telemetry.sdk.name' => 'flow-php-phpunit-telemetry',
                'telemetry.sdk.language' => 'php',
                'telemetry.sdk.version' => PackageVersion::get('flow-php/telemetry'),
            ]))
            ->merge(resource($config->resourceAttributes));

        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $transport = self::buildTransport($config->transport);
        $otlpExporter = otlp_exporter($transport, self::buildErrorHandler($config->errorHandler));
        $voidExporter = void_exporter();

        $spanProcessor = $config->emitTraces
            ? batching_span_processor($otlpExporter, $config->batchSize)
            : batching_span_processor($voidExporter, $config->batchSize);

        $metricProcessor = $config->emitMetrics
            ? batching_metric_processor($otlpExporter, $config->batchSize)
            : batching_metric_processor($voidExporter, $config->batchSize);

        $logProcessor = batching_log_processor($voidExporter, $config->batchSize);

        return telemetry(
            $telemetryResource,
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider($metricProcessor, $clock),
            logger_provider($logProcessor, $clock, $contextStorage),
        );
    }

    private static function buildCurlTransport(CurlTransportConfig $config): Transport
    {
        $options = otlp_curl_options()
            ->withTimeout($config->timeoutMs)
            ->withConnectTimeout($config->connectTimeoutMs)
            ->withShutdownTimeout($config->shutdownTimeoutMs)
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

    private static function buildErrorHandler(ErrorLogHandlerConfig|NullErrorHandlerConfig|StreamErrorHandlerConfig|SyslogErrorHandlerConfig|UdpSyslogErrorHandlerConfig $config): ErrorHandler
    {
        return match (true) {
            $config instanceof ErrorLogHandlerConfig => new ErrorLogHandler(
                messageType: $config->messageType,
                expandNewlines: $config->expandNewlines,
                messagePrefix: $config->messagePrefix,
            ),
            $config instanceof NullErrorHandlerConfig => new NullErrorHandler(),
            $config instanceof StreamErrorHandlerConfig => new StreamHandler(
                destination: $config->destination,
                filePermissions: $config->filePermissions,
                createDirectories: $config->createDirectories,
                messagePrefix: $config->messagePrefix,
            ),
            $config instanceof SyslogErrorHandlerConfig => new SyslogHandler(
                ident: $config->ident,
                facility: $config->facility,
                logOpts: $config->logOpts,
                severity: $config->severity,
            ),
            $config instanceof UdpSyslogErrorHandlerConfig => new UdpSyslogHandler(
                host: $config->host,
                port: $config->port,
                ident: $config->ident,
                facility: $config->facility,
                severity: $config->severity,
            ),
        };
    }

    private static function buildGrpcTransport(GrpcTransportConfig $config): Transport
    {
        return otlp_grpc_transport(
            endpoint: $config->endpoint,
            headers: $config->headers,
            insecure: $config->insecure,
            timeoutMs: $config->timeoutMs,
            shutdownTimeoutMs: $config->shutdownTimeoutMs,
        );
    }

    private static function buildStreamTransport(StreamTransportConfig $config): Transport
    {
        return otlp_stream_transport(
            destination: $config->destination,
            filePermissions: $config->filePermissions,
            createDirectories: $config->createDirectories,
        );
    }

    private static function buildTransport(CurlTransportConfig|GrpcTransportConfig|StreamTransportConfig $config): Transport
    {
        return match (true) {
            $config instanceof CurlTransportConfig => self::buildCurlTransport($config),
            $config instanceof GrpcTransportConfig => self::buildGrpcTransport($config),
            $config instanceof StreamTransportConfig => self::buildStreamTransport($config),
        };
    }
}
