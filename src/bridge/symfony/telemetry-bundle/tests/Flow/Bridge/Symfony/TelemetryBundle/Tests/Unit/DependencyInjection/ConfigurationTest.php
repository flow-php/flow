<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Configuration;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

final class ConfigurationTest extends TestCase
{
    public function test_composite_processor_with_multiple_processors() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'composite',
                    'processors' => [
                        [
                            'type' => 'memory',
                        ],
                        [
                            'type' => 'batching',
                            'batch_size' => 100,
                        ],
                    ],
                ],
            ],
        ]]);

        $processors = $config['tracer_provider']['processor']['processors'];
        self::assertCount(2, $processors);
        self::assertSame('memory', $processors[0]['type']);
        self::assertSame('batching', $processors[1]['type']);
        self::assertSame(100, $processors[1]['batch_size']);
    }

    public function test_empty_service_name_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => ''],
        ]]);
    }

    public function test_empty_tracers_meters_loggers_config() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracers' => [],
            'meters' => [],
            'loggers' => [],
        ]]);

        self::assertSame([], $config['tracers']);
        self::assertSame([], $config['meters']);
        self::assertSame([], $config['loggers']);
    }

    public function test_exporter_defaults_to_void() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                ],
            ],
        ]]);

        self::assertSame('void', $config['tracer_provider']['processor']['exporter']['type']);
    }

    public function test_invalid_exporter_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'invalid_exporter',
                    ],
                ],
            ],
        ]]);
    }

    public function test_invalid_processor_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'invalid_processor',
                ],
            ],
        ]]);
    }

    public function test_invalid_sampler_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'sampler' => [
                    'type' => 'invalid_sampler',
                ],
            ],
        ]]);
    }

    public function test_invalid_severity_level_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'logger_provider' => [
                'processor' => [
                    'type' => 'severity_filtering',
                    'minimum_severity' => 'invalid_level',
                    'inner_processor' => [
                        'type' => 'void',
                    ],
                ],
            ],
        ]]);
    }

    public function test_logger_configuration() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'loggers' => [
                'audit' => [
                    'version' => '1.0.0',
                    'schema_url' => 'https://example.com/audit-schema/1.0',
                    'attributes' => [
                        'log.category' => 'audit',
                    ],
                ],
            ],
        ]]);

        self::assertArrayHasKey('loggers', $config);
        self::assertArrayHasKey('audit', $config['loggers']);
        self::assertSame('1.0.0', $config['loggers']['audit']['version']);
        self::assertSame('https://example.com/audit-schema/1.0', $config['loggers']['audit']['schema_url']);
        self::assertSame(['log.category' => 'audit'], $config['loggers']['audit']['attributes']);
    }

    public function test_meter_configuration() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'meters' => [
                'etl_pipeline' => [
                    'version' => '1.0.0',
                    'attributes' => [
                        'flow.pipeline' => 'daily_import',
                    ],
                ],
            ],
        ]]);

        self::assertArrayHasKey('meters', $config);
        self::assertArrayHasKey('etl_pipeline', $config['meters']);
        self::assertSame('1.0.0', $config['meters']['etl_pipeline']['version']);
        self::assertNull($config['meters']['etl_pipeline']['schema_url']);
        self::assertSame(['flow.pipeline' => 'daily_import'], $config['meters']['etl_pipeline']['attributes']);
    }

    public function test_meter_provider_temporality_can_be_delta() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'meter_provider' => [
                'temporality' => 'delta',
            ],
        ]]);

        self::assertSame('delta', $config['meter_provider']['temporality']);
    }

    public function test_meter_provider_temporality_defaults_to_cumulative() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'meter_provider' => [],
        ]]);

        self::assertSame('cumulative', $config['meter_provider']['temporality']);
    }

    public function test_minimal_config_requires_service_name() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('service');

        (new Processor())->processConfiguration(new Configuration(), [[]]);
    }

    public function test_minimal_config_with_service_name() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertSame('test-app', $config['service']['name']);
        self::assertNull($config['service']['version']);
        self::assertSame([], $config['service']['attributes']);
    }

    public function test_multiple_named_items_of_same_type() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracers' => [
                'database' => [
                    'version' => '1.0.0',
                ],
                'http_client' => [
                    'version' => '2.0.0',
                ],
                'cache' => [],
            ],
        ]]);

        self::assertCount(3, $config['tracers']);
        self::assertSame('1.0.0', $config['tracers']['database']['version']);
        self::assertSame('2.0.0', $config['tracers']['http_client']['version']);
        self::assertSame('unknown', $config['tracers']['cache']['version']);
    }

    public function test_otlp_serializer_defaults_to_json() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [],
                        ],
                    ],
                ],
            ],
        ]]);

        $serializer = $config['tracer_provider']['processor']['exporter']['otlp']['transport']['serializer'];
        self::assertSame('json', $serializer['type']);
    }

    public function test_otlp_transport_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [],
                        ],
                    ],
                ],
            ],
        ]]);

        $transport = $config['tracer_provider']['processor']['exporter']['otlp']['transport'];
        self::assertSame('curl', $transport['type']);
        self::assertSame('http://localhost:4318', $transport['endpoint']);
        self::assertSame(30, $transport['timeout']);
        self::assertSame([], $transport['headers']);
        self::assertTrue($transport['insecure']);
    }

    public function test_processor_batch_size_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                ],
            ],
        ]]);

        self::assertSame(512, $config['tracer_provider']['processor']['batch_size']);
    }

    public function test_processor_batch_size_minimum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'batch_size' => 0,
                ],
            ],
        ]]);
    }

    public function test_processor_defaults_to_void() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [],
        ]]);

        self::assertSame('void', $config['tracer_provider']['processor']['type']);
    }

    public function test_providers_have_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('tracer_provider', $config);
        self::assertArrayHasKey('meter_provider', $config);
        self::assertArrayHasKey('logger_provider', $config);
        self::assertSame('void', $config['tracer_provider']['processor']['type']);
        self::assertSame('void', $config['meter_provider']['processor']['type']);
        self::assertSame('void', $config['logger_provider']['processor']['type']);
    }

    public function test_sampler_defaults_to_always_on() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [],
        ]]);

        self::assertSame('always_on', $config['tracer_provider']['sampler']['type']);
    }

    public function test_sampler_ratio_maximum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'sampler' => [
                    'type' => 'trace_id_ratio',
                    'ratio' => 1.1,
                ],
            ],
        ]]);
    }

    public function test_sampler_ratio_minimum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'sampler' => [
                    'type' => 'trace_id_ratio',
                    'ratio' => -0.1,
                ],
            ],
        ]]);
    }

    public function test_sampler_ratio_validation() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'sampler' => [
                    'type' => 'trace_id_ratio',
                    'ratio' => 0.5,
                ],
            ],
        ]]);

        self::assertSame(0.5, $config['tracer_provider']['sampler']['ratio']);
    }

    public function test_service_config_with_version_and_attributes() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => [
                'name' => 'test-app',
                'version' => '1.2.3',
                'attributes' => [
                    'environment' => 'production',
                    'region' => 'us-east-1',
                ],
            ],
        ]]);

        self::assertSame('test-app', $config['service']['name']);
        self::assertSame('1.2.3', $config['service']['version']);
        self::assertSame([
            'environment' => 'production',
            'region' => 'us-east-1',
        ], $config['service']['attributes']);
    }

    public function test_severity_filtering_is_only_available_for_log_processors() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'severity_filtering',
                ],
            ],
        ]]);
    }

    public function test_severity_filtering_minimum_severity_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'logger_provider' => [
                'processor' => [
                    'type' => 'severity_filtering',
                    'inner_processor' => [
                        'type' => 'void',
                    ],
                ],
            ],
        ]]);

        self::assertSame('info', $config['logger_provider']['processor']['minimum_severity']);
    }

    public function test_severity_filtering_processor_for_logs() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'logger_provider' => [
                'processor' => [
                    'type' => 'severity_filtering',
                    'minimum_severity' => 'warn',
                    'inner_processor' => [
                        'type' => 'batching',
                        'exporter' => ['type' => 'console'],
                    ],
                ],
            ],
        ]]);

        $processor = $config['logger_provider']['processor'];
        self::assertSame('severity_filtering', $processor['type']);
        self::assertSame('warn', $processor['minimum_severity']);
        self::assertSame('batching', $processor['inner_processor']['type']);
        self::assertSame('console', $processor['inner_processor']['exporter']['type']);
    }

    public function test_telemetry_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'http_kernel' => ['enabled' => true],
                'console' => ['enabled' => true],
                'messenger' => true,
            ],
        ]]);

        self::assertTrue($config['telemetry']['http_kernel']['enabled']);
        self::assertTrue($config['telemetry']['console']['enabled']);
        self::assertTrue($config['telemetry']['messenger']);
    }

    public function test_telemetry_console_exclude_commands() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'console' => [
                    'enabled' => true,
                    'exclude_commands' => ['cache:clear', 'debug:router'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['console']['enabled']);
        self::assertSame(['cache:clear', 'debug:router'], $config['telemetry']['console']['exclude_commands']);
    }

    public function test_telemetry_dbal_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'dbal' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['telemetry']['dbal']['enabled']);
    }

    public function test_telemetry_dbal_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('telemetry', $config);
        self::assertArrayHasKey('dbal', $config['telemetry']);
        self::assertFalse($config['telemetry']['dbal']['enabled']);
        self::assertTrue($config['telemetry']['dbal']['log_sql']);
        self::assertSame(1000, $config['telemetry']['dbal']['max_sql_length']);
        self::assertSame([], $config['telemetry']['dbal']['exclude_connections']);
    }

    public function test_telemetry_dbal_exclude_connections() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'dbal' => [
                    'enabled' => true,
                    'exclude_connections' => ['legacy', '/^debug_.*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['dbal']['enabled']);
        self::assertSame(['legacy', '/^debug_.*$/'], $config['telemetry']['dbal']['exclude_connections']);
    }

    public function test_telemetry_dbal_log_sql_can_be_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'dbal' => [
                    'enabled' => true,
                    'log_sql' => false,
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['dbal']['enabled']);
        self::assertFalse($config['telemetry']['dbal']['log_sql']);
    }

    public function test_telemetry_dbal_max_sql_length_can_be_zero_for_unlimited() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'dbal' => [
                    'enabled' => true,
                    'max_sql_length' => 0,
                ],
            ],
        ]]);

        self::assertSame(0, $config['telemetry']['dbal']['max_sql_length']);
    }

    public function test_telemetry_dbal_max_sql_length_configuration() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'dbal' => [
                    'enabled' => true,
                    'max_sql_length' => 500,
                ],
            ],
        ]]);

        self::assertSame(500, $config['telemetry']['dbal']['max_sql_length']);
    }

    public function test_telemetry_defaults_to_all_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('telemetry', $config);
        self::assertFalse($config['telemetry']['http_kernel']['enabled']);
        self::assertFalse($config['telemetry']['console']['enabled']);
        self::assertFalse($config['telemetry']['messenger']);
    }

    public function test_telemetry_http_client_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'http_client' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['telemetry']['http_client']['enabled']);
    }

    public function test_telemetry_http_client_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('telemetry', $config);
        self::assertArrayHasKey('http_client', $config['telemetry']);
        self::assertFalse($config['telemetry']['http_client']['enabled']);
        self::assertSame([], $config['telemetry']['http_client']['exclude_clients']);
    }

    public function test_telemetry_http_client_exclude_clients() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'http_client' => [
                    'enabled' => true,
                    'exclude_clients' => ['internal.http_client', '/^debug\\..*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['http_client']['enabled']);
        self::assertSame(['internal.http_client', '/^debug\\..*$/'], $config['telemetry']['http_client']['exclude_clients']);
    }

    public function test_telemetry_http_kernel_exclude_routes() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'http_kernel' => [
                    'enabled' => true,
                    'exclude_routes' => ['_wdt', '_profiler', '/_profiler.*/'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['http_kernel']['enabled']);
        self::assertSame(['_wdt', '_profiler', '/_profiler.*/'], $config['telemetry']['http_kernel']['exclude_routes']);
    }

    public function test_telemetry_partial_config() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'http_kernel' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['telemetry']['http_kernel']['enabled']);
        self::assertFalse($config['telemetry']['console']['enabled']);
        self::assertFalse($config['telemetry']['messenger']);
    }

    public function test_telemetry_psr18_client_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'psr18_client' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['telemetry']['psr18_client']['enabled']);
    }

    public function test_telemetry_psr18_client_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('telemetry', $config);
        self::assertArrayHasKey('psr18_client', $config['telemetry']);
        self::assertFalse($config['telemetry']['psr18_client']['enabled']);
        self::assertSame([], $config['telemetry']['psr18_client']['exclude_clients']);
    }

    public function test_telemetry_psr18_client_exclude_clients() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'psr18_client' => [
                    'enabled' => true,
                    'exclude_clients' => ['internal.psr18_client', '/^debug\\..*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['psr18_client']['enabled']);
        self::assertSame(['internal.psr18_client', '/^debug\\..*$/'], $config['telemetry']['psr18_client']['exclude_clients']);
    }

    public function test_telemetry_twig_exclude_templates() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'telemetry' => [
                'twig' => [
                    'enabled' => true,
                    'exclude_templates' => ['@WebProfiler/Collector/time.html.twig', 'debug/exception.html.twig'],
                ],
            ],
        ]]);

        self::assertTrue($config['telemetry']['twig']['enabled']);
        self::assertSame(['@WebProfiler/Collector/time.html.twig', 'debug/exception.html.twig'], $config['telemetry']['twig']['exclude_templates']);
    }

    public function test_tracer_configuration_with_all_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracers' => [
                'database' => [
                    'version' => '2.0.0',
                    'schema_url' => 'https://opentelemetry.io/schemas/1.20.0',
                    'attributes' => [
                        'db.system' => 'postgresql',
                        'db.pool_size' => 10,
                    ],
                ],
            ],
        ]]);

        self::assertArrayHasKey('tracers', $config);
        self::assertArrayHasKey('database', $config['tracers']);
        self::assertSame('2.0.0', $config['tracers']['database']['version']);
        self::assertSame('https://opentelemetry.io/schemas/1.20.0', $config['tracers']['database']['schema_url']);
        self::assertSame([
            'db.system' => 'postgresql',
            'db.pool_size' => 10,
        ], $config['tracers']['database']['attributes']);
    }

    public function test_tracer_configuration_with_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'tracers' => [
                'http_client' => [],
            ],
        ]]);

        self::assertArrayHasKey('tracers', $config);
        self::assertArrayHasKey('http_client', $config['tracers']);
        self::assertSame('unknown', $config['tracers']['http_client']['version']);
        self::assertNull($config['tracers']['http_client']['schema_url']);
        self::assertSame([], $config['tracers']['http_client']['attributes']);
    }
}
