<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Context\ConfigurationContext;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

use function sprintf;

final class ConfigurationTest extends TestCase
{
    private ConfigurationContext $context;

    protected function setUp(): void
    {
        $this->context = new ConfigurationContext();
    }

    public function test_clock_service_id_can_be_configured(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'clock_service_id' => 'app.custom_clock',
        ]);

        static::assertSame('app.custom_clock', $config['clock_service_id']);
    }

    public function test_git_detector_can_be_configured(): void
    {
        $config = $this->context->processConfig([
            'resource' => [
                'detectors' => [
                    'static' => [
                        'git' => [
                            'enabled' => true,
                            'binary' => '/usr/bin/git',
                            'working_directory' => '/srv/app',
                        ],
                    ],
                ],
            ],
        ]);

        $git = $config['resource']['detectors']['static']['git'];

        static::assertTrue($git['enabled']);
        static::assertSame('/usr/bin/git', $git['binary']);
        static::assertSame('/srv/app', $git['working_directory']);
    }

    public function test_git_detector_defaults_to_disabled(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        $git = $config['resource']['detectors']['static']['git'];

        static::assertFalse($git['enabled']);
        static::assertSame('git', $git['binary']);
        static::assertNull($git['working_directory']);
    }

    public function test_clock_service_id_defaults_to_null(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertArrayHasKey('clock_service_id', $config);
        static::assertNull($config['clock_service_id']);
    }

    public function test_composite_span_processor_with_named_exporters(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'memory' => ['memory' => null],
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'composite',
                    'processors' => [
                        ['type' => 'memory', 'exporter' => 'memory'],
                        ['type' => 'batching', 'batch_size' => 100, 'exporter' => 'otlp'],
                    ],
                ],
            ],
        ]);

        $processors = $config['tracer_provider']['processor']['processors'];
        static::assertCount(2, $processors);
        static::assertSame('memory', $processors[0]['exporter']);
        static::assertSame('otlp', $processors[1]['exporter']);
    }

    public function test_console_exporter_no_options(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'console' => ['console' => null],
            ],
        ]);

        static::assertArrayHasKey('console', $config['exporters']['console']);
    }

    public function test_context_storage_can_use_custom_service(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'context_storage' => [
                'type' => 'service',
                'service_id' => 'app.custom_context_storage',
            ],
        ]);

        static::assertSame('service', $config['context_storage']['type']);
        static::assertSame('app.custom_context_storage', $config['context_storage']['service_id']);
    }

    public function test_context_storage_defaults_to_memory(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertArrayHasKey('context_storage', $config);
        static::assertSame('memory', $config['context_storage']['type']);
        static::assertNull($config['context_storage']['service_id']);
    }

    public function test_empty_exporters_definition(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertSame([], $config['exporters']);
    }

    public function test_error_handlers_can_define_multiple_named_entries(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'error_handlers' => [
                'default' => ['type' => 'error_log', 'message_type' => 'sapi'],
                'to_file' => ['type' => 'stream', 'destination' => '/var/log/flow.log'],
                'to_syslog' => ['type' => 'syslog', 'facility' => 'local0', 'severity' => 'warning'],
                'fanout' => ['type' => 'composite', 'handlers' => ['default', 'to_file']],
                'silent' => ['type' => 'noop'],
                'custom' => ['type' => 'service', 'service_id' => 'app.my_handler'],
            ],
        ]);

        static::assertSame('error_log', $config['error_handlers']['default']['type']);
        static::assertSame('sapi', $config['error_handlers']['default']['message_type']);
        static::assertSame('stream', $config['error_handlers']['to_file']['type']);
        static::assertSame('/var/log/flow.log', $config['error_handlers']['to_file']['destination']);
        static::assertSame('local0', $config['error_handlers']['to_syslog']['facility']);
        static::assertSame('warning', $config['error_handlers']['to_syslog']['severity']);
        static::assertSame(['default', 'to_file'], $config['error_handlers']['fanout']['handlers']);
        static::assertSame('noop', $config['error_handlers']['silent']['type']);
        static::assertSame('app.my_handler', $config['error_handlers']['custom']['service_id']);
    }

    public function test_error_handlers_section_is_empty_by_default(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertArrayHasKey('error_handlers', $config);
        static::assertSame([], $config['error_handlers']);
    }

    public function test_exporter_with_multiple_blocks_throws(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Exporter must declare exactly one of');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'memory' => null,
                    'console' => null,
                ],
            ],
        ]);
    }

    public function test_exporter_with_no_block_throws(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Exporter must declare exactly one of');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'broken' => [],
            ],
        ]);
    }

    public function test_grpc_transport_accepts_timeout_ms(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'grpc',
                            'endpoint' => 'localhost:4317',
                            'timeout_ms' => 2000,
                        ],
                    ],
                ],
            ],
        ]);

        static::assertSame(2000, $config['exporters']['otlp']['otlp']['transport']['timeout_ms']);
    }

    public function test_grpc_transport_rejects_connect_timeout_ms(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The "connect_timeout_ms" parameter is not supported when transport.type is "grpc"',
        );

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'grpc',
                            'endpoint' => 'localhost:4317',
                            'connect_timeout_ms' => 250,
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function test_grpc_transport_rejects_encoding_field(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('"encoding" parameter is not supported when transport.type is "grpc"');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_grpc' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'grpc',
                            'endpoint' => 'localhost:4317',
                            'encoding' => 'json',
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function test_minimal_otlp_config(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'curl',
                            'endpoint' => 'http://localhost:4318',
                            'encoding' => 'protobuf',
                        ],
                    ],
                ],
            ],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp', 'batch_size' => 512],
            ],
        ]);

        static::assertSame('curl', $config['exporters']['otlp']['otlp']['transport']['type']);
        static::assertSame('http://localhost:4318', $config['exporters']['otlp']['otlp']['transport']['endpoint']);
        static::assertSame('protobuf', $config['exporters']['otlp']['otlp']['transport']['encoding']);
        static::assertSame('otlp', $config['tracer_provider']['processor']['exporter']);
    }

    public function test_otlp_exporter_error_handler_defaults_to_default(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]);

        static::assertSame('default', $config['exporters']['otlp']['otlp']['error_handler']);
    }

    public function test_otlp_exporter_without_transport_block_throws(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('OTLP exporter requires a "transport" configuration block');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'otlp' => [],
                ],
            ],
        ]);
    }

    public function test_processor_error_handler_defaults_to_default(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertSame('default', $config['tracer_provider']['processor']['error_handler']);
        static::assertSame('default', $config['meter_provider']['processor']['error_handler']);
        static::assertSame('default', $config['logger_provider']['processor']['error_handler']);
    }

    public function test_propagator_defaults_to_w3c(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertSame('w3c', $config['propagator']['type']);
    }

    public function test_provider_error_handler_defaults_to_default(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
        ]);

        static::assertSame('default', $config['tracer_provider']['error_handler']);
        static::assertSame('default', $config['meter_provider']['error_handler']);
        static::assertSame('default', $config['logger_provider']['error_handler']);
    }

    public function test_service_exporter_requires_id(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('id');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'service' => [],
                ],
            ],
        ]);
    }

    public function test_service_exporter_with_id(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'datadog' => [
                    'service' => ['id' => 'app.datadog_exporter'],
                ],
            ],
        ]);

        static::assertSame('app.datadog_exporter', $config['exporters']['datadog']['service']['id']);
    }

    public function test_log_pipeline_with_severity_middleware_and_batching_sink(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
            'logger_provider' => [
                'processor' => [
                    'type' => 'pipeline',
                    'middleware' => [
                        ['type' => 'severity_filtering', 'minimum_severity' => 'warn'],
                    ],
                    'sink' => [
                        'type' => 'batching',
                        'exporter' => 'otlp',
                        'batch_size' => 200,
                    ],
                ],
            ],
        ]);

        static::assertSame('pipeline', $config['logger_provider']['processor']['type']);
        static::assertSame('severity_filtering', $config['logger_provider']['processor']['middleware'][0]['type']);
        static::assertSame('warn', $config['logger_provider']['processor']['middleware'][0]['minimum_severity']);
        static::assertSame('otlp', $config['logger_provider']['processor']['sink']['exporter']);
        static::assertSame(200, $config['logger_provider']['processor']['sink']['batch_size']);
    }

    public function test_stream_transport_accepts_file_options(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'stream',
                            'endpoint' => '/var/log/otel/logs.jsonl',
                            'file_permissions' => 0o640,
                            'create_directories' => false,
                        ],
                    ],
                ],
            ],
        ]);

        $transport = $config['exporters']['otlp_stream']['otlp']['transport'];
        static::assertSame(0o640, $transport['file_permissions']);
        static::assertFalse($transport['create_directories']);
    }

    #[TestWith(['/var/log/otel/logs.jsonl'])]
    #[TestWith(['php://stdout'])]
    #[TestWith(['php://stderr'])]
    public function test_stream_transport_accepts_file_path_and_php_uri(string $endpoint): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'stream',
                            'endpoint' => $endpoint,
                        ],
                    ],
                ],
            ],
        ]);

        $transport = $config['exporters']['otlp_stream']['otlp']['transport'];
        static::assertSame('stream', $transport['type']);
        static::assertSame($endpoint, $transport['endpoint']);
        static::assertSame(0644, $transport['file_permissions']);
        static::assertTrue($transport['create_directories']);
    }

    public function test_stream_transport_rejects_encoding_field(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('"encoding" parameter is not supported when transport.type is "stream"');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'stream',
                            'endpoint' => '/var/log/otel/logs.jsonl',
                            'encoding' => 'protobuf',
                        ],
                    ],
                ],
            ],
        ]);
    }

    #[TestWith(['timeout_ms', 2000])]
    #[TestWith(['connect_timeout_ms', 500])]
    #[TestWith(['compression', true])]
    #[TestWith(['ssl_cert_path', '/etc/cert.pem'])]
    #[TestWith(['headers', ['Authorization' => 'Bearer x']])]
    #[TestWith(['insecure', true])]
    public function test_stream_transport_rejects_http_specific_options(string $key, mixed $value): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(sprintf(
            'The "%s" parameter is not supported when transport.type is "stream".',
            $key,
        ));

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'stream',
                            'endpoint' => '/var/log/otel/logs.jsonl',
                            $key => $value,
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function test_stream_transport_requires_non_empty_endpoint(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('endpoint');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => ['type' => 'stream'],
                    ],
                ],
            ],
        ]);
    }

    public function test_transport_failover_block_is_accepted_under_curl_primary(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'curl',
                            'endpoint' => 'http://localhost:4318',
                            'failover' => [
                                'type' => 'stream',
                                'endpoint' => 'php://memory',
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        static::assertSame('stream', $config['exporters']['otlp']['otlp']['transport']['failover']['type']);
        static::assertSame('php://memory', $config['exporters']['otlp']['otlp']['transport']['failover']['endpoint']);
    }

    public function test_transport_failover_rejected_for_stream_primary(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('"failover" block is only supported for transport.type "curl" or "grpc"');

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'stream',
                            'endpoint' => 'php://memory',
                            'failover' => [
                                'type' => 'stream',
                                'endpoint' => 'php://memory',
                            ],
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function test_transport_failover_rejects_nested_failover(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Unrecognized option "failover" under "flow_telemetry.exporters.otlp.otlp.transport.failover"',
        );

        $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'curl',
                            'endpoint' => 'http://localhost:4318',
                            'failover' => [
                                'type' => 'stream',
                                'endpoint' => 'php://memory',
                                'failover' => [
                                    'type' => 'stream',
                                    'endpoint' => 'php://memory',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]);
    }

    public function test_transport_service_type_inside_otlp(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'exporters' => [
                'custom_otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'service', 'service_id' => 'app.my_transport'],
                    ],
                ],
            ],
        ]);

        static::assertSame('service', $config['exporters']['custom_otlp']['otlp']['transport']['type']);
        static::assertSame('app.my_transport', $config['exporters']['custom_otlp']['otlp']['transport']['service_id']);
    }

    public function test_messenger_link_to_worker_defaults_to_true_and_is_configurable(): void
    {
        $default = $this->context->processConfig([
            'resource' => [],
            'instrumentation' => ['messenger' => ['enabled' => true]],
        ]);
        static::assertTrue($default['instrumentation']['messenger']['link_to_worker']);

        $disabled = $this->context->processConfig([
            'resource' => [],
            'instrumentation' => ['messenger' => ['enabled' => true, 'link_to_worker' => false]],
        ]);
        static::assertFalse($disabled['instrumentation']['messenger']['link_to_worker']);
    }

    public function test_max_batch_age_is_parsed_for_span_metric_and_log_processors(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp', 'max_batch_age' => 15.0],
            ],
            'meter_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp', 'max_batch_age' => 30.0],
            ],
            'logger_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp', 'max_batch_age' => 5.5],
            ],
        ]);

        static::assertSame(15.0, $config['tracer_provider']['processor']['max_batch_age']);
        static::assertSame(30.0, $config['meter_provider']['processor']['max_batch_age']);
        static::assertSame(5.5, $config['logger_provider']['processor']['max_batch_age']);
    }

    public function test_max_batch_age_defaults_to_null_when_omitted(): void
    {
        $config = $this->context->processConfig([
            'resource' => [],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp'],
            ],
            'meter_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp'],
            ],
            'logger_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'otlp'],
            ],
        ]);

        static::assertArrayHasKey('max_batch_age', $config['tracer_provider']['processor']);
        static::assertNull($config['tracer_provider']['processor']['max_batch_age']);
        static::assertNull($config['meter_provider']['processor']['max_batch_age']);
        static::assertNull($config['logger_provider']['processor']['max_batch_age']);
    }
}
