<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\CurlTransportConfig;
use Flow\Bridge\PHPUnit\Telemetry\ErrorLogHandlerConfig;
use Flow\Bridge\PHPUnit\Telemetry\GrpcTransportConfig;
use Flow\Bridge\PHPUnit\Telemetry\NullErrorHandlerConfig;
use Flow\Bridge\PHPUnit\Telemetry\SerializerType;
use Flow\Bridge\PHPUnit\Telemetry\StreamErrorHandlerConfig;
use Flow\Bridge\PHPUnit\Telemetry\StreamTransportConfig;
use Flow\Bridge\PHPUnit\Telemetry\SyslogErrorHandlerConfig;
use Flow\Bridge\PHPUnit\Telemetry\TelemetryFactory;
use Flow\Bridge\PHPUnit\Telemetry\Tests\Mother\ConfigurationMother;
use Flow\Bridge\PHPUnit\Telemetry\UdpSyslogErrorHandlerConfig;
use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;
use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\TestCase;

final class TelemetryFactoryTest extends TestCase
{
    public function test_create_returns_telemetry_with_default_curl_transport(): void
    {
        $telemetry = TelemetryFactory::create(ConfigurationMother::default());

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_disabled_metrics_routes_metrics_to_void(): void
    {
        $telemetry = TelemetryFactory::create(ConfigurationMother::withDisabledMetrics());

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_disabled_traces_routes_spans_to_void(): void
    {
        $telemetry = TelemetryFactory::create(ConfigurationMother::withDisabledTraces());

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_error_log_handler(): void
    {
        $config = $this->configWithErrorHandler(new ErrorLogHandlerConfig(
            messageType: ErrorLogMessageType::Sapi,
            expandNewlines: true,
            messagePrefix: '[test]',
        ));

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_full_curl_options_covers_optional_branches(): void
    {
        $config = new Configuration(
            serviceName: 'phpunit',
            transport: new CurlTransportConfig(
                endpoint: 'http://localhost:4318',
                headers: ['Authorization' => 'Bearer token', 'X-Custom' => 'value'],
                timeoutMs: 250,
                connectTimeoutMs: 250,
                shutdownTimeoutMs: 5000,
                compression: true,
                followRedirects: true,
                maxRedirects: 5,
                proxy: 'http://proxy:8080',
                sslVerifyPeer: false,
                sslVerifyHost: false,
                sslCertPath: '/tmp/cert.pem',
                sslKeyPath: '/tmp/key.pem',
                caInfoPath: '/tmp/ca.pem',
                serializer: SerializerType::PROTOBUF,
            ),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: 256,
            errorHandler: ConfigurationMother::defaultErrorHandler(),
        );

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_grpc_transport(): void
    {
        if (!\extension_loaded('grpc')) {
            static::markTestSkipped('grpc extension is required');
        }

        $config = new Configuration(
            serviceName: 'phpunit',
            transport: new GrpcTransportConfig(
                endpoint: 'localhost:4317',
                headers: ['api-key' => 'x'],
                insecure: true,
                timeoutMs: 250,
                shutdownTimeoutMs: 5000,
            ),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: ConfigurationMother::defaultErrorHandler(),
        );

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_null_error_handler(): void
    {
        $config = $this->configWithErrorHandler(new NullErrorHandlerConfig());

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_stream_error_handler(): void
    {
        $config = $this->configWithErrorHandler(new StreamErrorHandlerConfig(
            destination: 'php://memory',
            filePermissions: 0644,
            createDirectories: true,
            messagePrefix: '[test]',
        ));

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_stream_transport(): void
    {
        $config = new Configuration(
            serviceName: 'phpunit',
            transport: new StreamTransportConfig(
                destination: 'php://memory',
                filePermissions: 0644,
                createDirectories: true,
            ),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: ConfigurationMother::defaultErrorHandler(),
        );

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_syslog_error_handler(): void
    {
        $config = $this->configWithErrorHandler(new SyslogErrorHandlerConfig(
            ident: 'flow-test',
            facility: SyslogFacility::Local0,
            logOpts: \LOG_PID,
            severity: SyslogSeverity::Warning,
        ));

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    public function test_create_with_udp_syslog_error_handler(): void
    {
        $config = $this->configWithErrorHandler(new UdpSyslogErrorHandlerConfig(
            host: '127.0.0.1',
            port: 514,
            ident: 'flow-test',
            facility: SyslogFacility::User,
            severity: SyslogSeverity::Info,
        ));

        $telemetry = TelemetryFactory::create($config);

        static::assertInstanceOf(Telemetry::class, $telemetry);
        $telemetry->shutdown();
    }

    private function configWithErrorHandler(ErrorLogHandlerConfig|NullErrorHandlerConfig|StreamErrorHandlerConfig|SyslogErrorHandlerConfig|UdpSyslogErrorHandlerConfig $errorHandler): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: ConfigurationMother::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: $errorHandler,
        );
    }
}
