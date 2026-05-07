<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Configuration;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

final class ConfigurationTest extends TestCase
{
    public function test_clock_service_id_can_be_configured() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'clock_service_id' => 'app.custom_clock',
        ]]);

        self::assertSame('app.custom_clock', $config['clock_service_id']);
    }

    public function test_clock_service_id_defaults_to_null() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('clock_service_id', $config);
        self::assertNull($config['clock_service_id']);
    }

    public function test_composite_span_processor_with_named_exporters() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $processors = $config['tracer_provider']['processor']['processors'];
        self::assertCount(2, $processors);
        self::assertSame('memory', $processors[0]['exporter']);
        self::assertSame('otlp', $processors[1]['exporter']);
    }

    public function test_console_exporter_no_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'console' => ['console' => null],
            ],
        ]]);

        self::assertArrayHasKey('console', $config['exporters']['console']);
    }

    public function test_context_storage_can_use_custom_service() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'context_storage' => [
                'type' => 'service',
                'service_id' => 'app.custom_context_storage',
            ],
        ]]);

        self::assertSame('service', $config['context_storage']['type']);
        self::assertSame('app.custom_context_storage', $config['context_storage']['service_id']);
    }

    public function test_context_storage_defaults_to_memory() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('context_storage', $config);
        self::assertSame('memory', $config['context_storage']['type']);
        self::assertNull($config['context_storage']['service_id']);
    }

    public function test_empty_exporters_definition() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertSame([], $config['exporters']);
    }

    public function test_error_handlers_can_define_multiple_named_entries() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'error_handlers' => [
                'default' => ['type' => 'error_log', 'message_type' => 'sapi'],
                'to_file' => ['type' => 'stream', 'destination' => '/var/log/flow.log'],
                'to_syslog' => ['type' => 'syslog', 'facility' => 'local0', 'severity' => 'warning'],
                'fanout' => ['type' => 'composite', 'handlers' => ['default', 'to_file']],
                'silent' => ['type' => 'noop'],
                'custom' => ['type' => 'service', 'service_id' => 'app.my_handler'],
            ],
        ]]);

        self::assertSame('error_log', $config['error_handlers']['default']['type']);
        self::assertSame('sapi', $config['error_handlers']['default']['message_type']);
        self::assertSame('stream', $config['error_handlers']['to_file']['type']);
        self::assertSame('/var/log/flow.log', $config['error_handlers']['to_file']['destination']);
        self::assertSame('local0', $config['error_handlers']['to_syslog']['facility']);
        self::assertSame('warning', $config['error_handlers']['to_syslog']['severity']);
        self::assertSame(['default', 'to_file'], $config['error_handlers']['fanout']['handlers']);
        self::assertSame('noop', $config['error_handlers']['silent']['type']);
        self::assertSame('app.my_handler', $config['error_handlers']['custom']['service_id']);
    }

    public function test_error_handlers_section_is_empty_by_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('error_handlers', $config);
        self::assertSame([], $config['error_handlers']);
    }

    public function test_exporter_with_multiple_blocks_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Exporter must declare exactly one of');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'memory' => null,
                    'console' => null,
                ],
            ],
        ]]);
    }

    public function test_exporter_with_no_block_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Exporter must declare exactly one of');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'broken' => [],
            ],
        ]]);
    }

    public function test_grpc_transport_rejects_encoding_field() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('"encoding" parameter is not supported when transport.type is "grpc"');

        (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);
    }

    public function test_grpc_transport_with_timeout_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The "timeout" parameter is not supported when transport.type is "grpc"');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => [
                            'type' => 'grpc',
                            'endpoint' => 'localhost:4317',
                            'timeout' => 30,
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_minimal_otlp_config() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        self::assertSame('curl', $config['exporters']['otlp']['otlp']['transport']['type']);
        self::assertSame('http://localhost:4318', $config['exporters']['otlp']['otlp']['transport']['endpoint']);
        self::assertSame('protobuf', $config['exporters']['otlp']['otlp']['transport']['encoding']);
        self::assertSame('otlp', $config['tracer_provider']['processor']['exporter']);
    }

    public function test_otlp_exporter_error_handler_defaults_to_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]]);

        self::assertSame('default', $config['exporters']['otlp']['otlp']['error_handler']);
    }

    public function test_otlp_exporter_without_transport_block_throws() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('OTLP exporter requires a "transport" configuration block');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'otlp' => [],
                ],
            ],
        ]]);
    }

    public function test_processor_error_handler_defaults_to_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertSame('default', $config['tracer_provider']['processor']['error_handler']);
        self::assertSame('default', $config['meter_provider']['processor']['error_handler']);
        self::assertSame('default', $config['logger_provider']['processor']['error_handler']);
    }

    public function test_propagator_defaults_to_w3c() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertSame('w3c', $config['propagator']['type']);
    }

    public function test_provider_error_handler_defaults_to_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertSame('default', $config['tracer_provider']['error_handler']);
        self::assertSame('default', $config['meter_provider']['error_handler']);
        self::assertSame('default', $config['logger_provider']['error_handler']);
    }

    public function test_service_exporter_requires_id() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('id');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'broken' => [
                    'service' => [],
                ],
            ],
        ]]);
    }

    public function test_service_exporter_with_id() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'datadog' => [
                    'service' => ['id' => 'app.datadog_exporter'],
                ],
            ],
        ]]);

        self::assertSame('app.datadog_exporter', $config['exporters']['datadog']['service']['id']);
    }

    public function test_severity_filtering_wraps_batching() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
                    'type' => 'severity_filtering',
                    'minimum_severity' => 'warn',
                    'inner_processor' => [
                        'type' => 'batching',
                        'exporter' => 'otlp',
                        'batch_size' => 200,
                    ],
                ],
            ],
        ]]);

        self::assertSame('severity_filtering', $config['logger_provider']['processor']['type']);
        self::assertSame('warn', $config['logger_provider']['processor']['minimum_severity']);
        self::assertSame('otlp', $config['logger_provider']['processor']['inner_processor']['exporter']);
    }

    public function test_stream_transport_accepts_file_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $transport = $config['exporters']['otlp_stream']['otlp']['transport'];
        self::assertSame(0o640, $transport['file_permissions']);
        self::assertFalse($transport['create_directories']);
    }

    #[TestWith(['/var/log/otel/logs.jsonl'])]
    #[TestWith(['php://stdout'])]
    #[TestWith(['php://stderr'])]
    public function test_stream_transport_accepts_file_path_and_php_uri(string $endpoint) : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);

        $transport = $config['exporters']['otlp_stream']['otlp']['transport'];
        self::assertSame('stream', $transport['type']);
        self::assertSame($endpoint, $transport['endpoint']);
        self::assertSame(0644, $transport['file_permissions']);
        self::assertTrue($transport['create_directories']);
    }

    public function test_stream_transport_rejects_encoding_field() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('"encoding" parameter is not supported when transport.type is "stream"');

        (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);
    }

    #[TestWith(['timeout', 30])]
    #[TestWith(['connect_timeout', 5])]
    #[TestWith(['compression', true])]
    #[TestWith(['ssl_cert_path', '/etc/cert.pem'])]
    #[TestWith(['headers', ['Authorization' => 'Bearer x']])]
    #[TestWith(['insecure', true])]
    public function test_stream_transport_rejects_http_specific_options(string $key, mixed $value) : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(\sprintf('The "%s" parameter is not supported when transport.type is "stream".', $key));

        (new Processor())->processConfiguration(new Configuration(), [[
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
        ]]);
    }

    public function test_stream_transport_requires_non_empty_endpoint() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('endpoint');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'otlp_stream' => [
                    'otlp' => [
                        'transport' => ['type' => 'stream'],
                    ],
                ],
            ],
        ]]);
    }

    public function test_transport_service_type_inside_otlp() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'exporters' => [
                'custom_otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'service', 'service_id' => 'app.my_transport'],
                    ],
                ],
            ],
        ]]);

        self::assertSame('service', $config['exporters']['custom_otlp']['otlp']['transport']['type']);
        self::assertSame('app.my_transport', $config['exporters']['custom_otlp']['otlp']['transport']['service_id']);
    }
}
