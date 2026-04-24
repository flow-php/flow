<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Configuration;
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

    public function test_composite_processor_with_multiple_processors() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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

    public function test_empty_tracers_meters_loggers_config() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                ],
            ],
        ]]);

        self::assertSame('void', $config['tracer_provider']['processor']['exporter']['type']);
    }

    public function test_instrumentation_cache_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'cache' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['cache']['enabled']);
    }

    public function test_instrumentation_cache_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('instrumentation', $config);
        self::assertArrayHasKey('cache', $config['instrumentation']);
        self::assertFalse($config['instrumentation']['cache']['enabled']);
        self::assertSame([], $config['instrumentation']['cache']['exclude_pools']);
    }

    public function test_instrumentation_cache_exclude_pools() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'cache' => [
                    'enabled' => true,
                    'exclude_pools' => ['cache.validator', 'cache.serializer', '/^cache\\.profiler\\..*/'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['cache']['enabled']);
        self::assertSame(['cache.validator', 'cache.serializer', '/^cache\\.profiler\\..*/'], $config['instrumentation']['cache']['exclude_pools']);
    }

    public function test_instrumentation_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'http_kernel' => ['enabled' => true],
                'console' => ['enabled' => true],
                'messenger' => true,
            ],
        ]]);

        self::assertTrue($config['instrumentation']['http_kernel']['enabled']);
        self::assertTrue($config['instrumentation']['console']['enabled']);
        self::assertTrue($config['instrumentation']['messenger']['enabled']);
    }

    public function test_instrumentation_console_exclude_commands() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'console' => [
                    'enabled' => true,
                    'exclude_commands' => ['cache:clear', 'debug:router'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['console']['enabled']);
        self::assertSame(['cache:clear', 'debug:router'], $config['instrumentation']['console']['exclude_commands']);
    }

    public function test_instrumentation_dbal_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'dbal' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['dbal']['enabled']);
    }

    public function test_instrumentation_dbal_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('instrumentation', $config);
        self::assertArrayHasKey('dbal', $config['instrumentation']);
        self::assertFalse($config['instrumentation']['dbal']['enabled']);
        self::assertTrue($config['instrumentation']['dbal']['log_sql']);
        self::assertSame(1000, $config['instrumentation']['dbal']['max_sql_length']);
        self::assertSame([], $config['instrumentation']['dbal']['exclude_connections']);
    }

    public function test_instrumentation_dbal_exclude_connections() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'dbal' => [
                    'enabled' => true,
                    'exclude_connections' => ['legacy', '/^debug_.*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['dbal']['enabled']);
        self::assertSame(['legacy', '/^debug_.*$/'], $config['instrumentation']['dbal']['exclude_connections']);
    }

    public function test_instrumentation_dbal_log_sql_can_be_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'dbal' => [
                    'enabled' => true,
                    'log_sql' => false,
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['dbal']['enabled']);
        self::assertFalse($config['instrumentation']['dbal']['log_sql']);
    }

    public function test_instrumentation_dbal_max_sql_length_can_be_zero_for_unlimited() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'dbal' => [
                    'enabled' => true,
                    'max_sql_length' => 0,
                ],
            ],
        ]]);

        self::assertSame(0, $config['instrumentation']['dbal']['max_sql_length']);
    }

    public function test_instrumentation_dbal_max_sql_length_configuration() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'dbal' => [
                    'enabled' => true,
                    'max_sql_length' => 500,
                ],
            ],
        ]]);

        self::assertSame(500, $config['instrumentation']['dbal']['max_sql_length']);
    }

    public function test_instrumentation_defaults_to_all_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('instrumentation', $config);
        self::assertFalse($config['instrumentation']['http_kernel']['enabled']);
        self::assertSame([], $config['instrumentation']['http_kernel']['exclude_paths']);
        self::assertFalse($config['instrumentation']['console']['enabled']);
        self::assertFalse($config['instrumentation']['messenger']['enabled']);
    }

    public function test_instrumentation_http_client_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'http_client' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['http_client']['enabled']);
    }

    public function test_instrumentation_http_client_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('instrumentation', $config);
        self::assertArrayHasKey('http_client', $config['instrumentation']);
        self::assertFalse($config['instrumentation']['http_client']['enabled']);
        self::assertSame([], $config['instrumentation']['http_client']['exclude_clients']);
    }

    public function test_instrumentation_http_client_exclude_clients() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'http_client' => [
                    'enabled' => true,
                    'exclude_clients' => ['internal.http_client', '/^debug\\..*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['http_client']['enabled']);
        self::assertSame(['internal.http_client', '/^debug\\..*$/'], $config['instrumentation']['http_client']['exclude_clients']);
    }

    public function test_instrumentation_http_kernel_exclude_paths() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'http_kernel' => [
                    'enabled' => true,
                    'exclude_paths' => [
                        ['path' => '/_wdt'],
                        ['path' => '/_profiler', 'method' => 'GET'],
                        ['path' => '/^\/_profiler.*/'],
                    ],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['http_kernel']['enabled']);
        self::assertSame([
            ['path' => '/_wdt', 'method' => null],
            ['path' => '/_profiler', 'method' => 'GET'],
            ['path' => '/^\/_profiler.*/', 'method' => null],
        ], $config['instrumentation']['http_kernel']['exclude_paths']);
    }

    public function test_instrumentation_partial_config() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'http_kernel' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['http_kernel']['enabled']);
        self::assertFalse($config['instrumentation']['console']['enabled']);
        self::assertFalse($config['instrumentation']['messenger']['enabled']);
    }

    public function test_instrumentation_psr18_client_can_be_enabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'psr18_client' => ['enabled' => true],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['psr18_client']['enabled']);
    }

    public function test_instrumentation_psr18_client_defaults_to_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('instrumentation', $config);
        self::assertArrayHasKey('psr18_client', $config['instrumentation']);
        self::assertFalse($config['instrumentation']['psr18_client']['enabled']);
        self::assertSame([], $config['instrumentation']['psr18_client']['exclude_clients']);
    }

    public function test_instrumentation_psr18_client_exclude_clients() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'psr18_client' => [
                    'enabled' => true,
                    'exclude_clients' => ['internal.psr18_client', '/^debug\\..*$/'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['psr18_client']['enabled']);
        self::assertSame(['internal.psr18_client', '/^debug\\..*$/'], $config['instrumentation']['psr18_client']['exclude_clients']);
    }

    public function test_instrumentation_twig_exclude_templates() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'instrumentation' => [
                'twig' => [
                    'enabled' => true,
                    'exclude_templates' => ['@WebProfiler/Collector/time.html.twig', 'debug/exception.html.twig'],
                ],
            ],
        ]]);

        self::assertTrue($config['instrumentation']['twig']['enabled']);
        self::assertSame(['@WebProfiler/Collector/time.html.twig', 'debug/exception.html.twig'], $config['instrumentation']['twig']['exclude_templates']);
    }

    public function test_invalid_exporter_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
            'meter_provider' => [
                'temporality' => 'delta',
            ],
        ]]);

        self::assertSame('delta', $config['meter_provider']['temporality']);
    }

    public function test_meter_provider_temporality_defaults_to_cumulative() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'meter_provider' => [],
        ]]);

        self::assertSame('cumulative', $config['meter_provider']['temporality']);
    }

    public function test_minimal_config_with_empty_resource() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('resource', $config);
        self::assertSame([], $config['resource']['custom']);
        self::assertTrue($config['resource']['detectors']['enabled']);
    }

    public function test_multiple_named_items_of_same_type() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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

    public function test_otlp_curl_transport_accepts_explicit_timeout() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'curl',
                                'endpoint' => 'http://otel:4318',
                                'timeout' => 60,
                            ],
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame(60, $config['tracer_provider']['processor']['exporter']['otlp']['transport']['timeout']);
    }

    public function test_otlp_curl_transport_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'curl',
                                'endpoint' => 'http://localhost:4318',
                                'timeout' => 60,
                                'connect_timeout' => 5,
                                'compression' => true,
                                'follow_redirects' => false,
                                'max_redirects' => 5,
                                'proxy' => 'http://proxy:8080',
                                'ssl_verify_peer' => false,
                                'ssl_verify_host' => false,
                                'ssl_cert_path' => '/path/to/cert.pem',
                                'ssl_key_path' => '/path/to/key.pem',
                                'ca_info_path' => '/path/to/ca.pem',
                            ],
                        ],
                    ],
                ],
            ],
        ]]);

        $transport = $config['tracer_provider']['processor']['exporter']['otlp']['transport'];
        self::assertSame('curl', $transport['type']);
        self::assertSame('http://localhost:4318', $transport['endpoint']);
        self::assertSame(60, $transport['timeout']);
        self::assertSame(5, $transport['connect_timeout']);
        self::assertTrue($transport['compression']);
        self::assertFalse($transport['follow_redirects']);
        self::assertSame(5, $transport['max_redirects']);
        self::assertSame('http://proxy:8080', $transport['proxy']);
        self::assertFalse($transport['ssl_verify_peer']);
        self::assertFalse($transport['ssl_verify_host']);
        self::assertSame('/path/to/cert.pem', $transport['ssl_cert_path']);
        self::assertSame('/path/to/key.pem', $transport['ssl_key_path']);
        self::assertSame('/path/to/ca.pem', $transport['ca_info_path']);
    }

    public function test_otlp_endpoint_is_required() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('endpoint');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'curl',
                            ],
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_otlp_grpc_transport_accepts_config_without_timeout() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'grpc',
                                'endpoint' => 'otel:4317',
                                'insecure' => false,
                            ],
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame('grpc', $config['tracer_provider']['processor']['exporter']['otlp']['transport']['type']);
        self::assertSame('otel:4317', $config['tracer_provider']['processor']['exporter']['otlp']['transport']['endpoint']);
        self::assertFalse($config['tracer_provider']['processor']['exporter']['otlp']['transport']['insecure']);
    }

    public function test_otlp_grpc_transport_rejects_explicit_timeout() : void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The "timeout" parameter is not supported when transport.type is "grpc"');

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'grpc',
                                'endpoint' => 'otel:4317',
                                'timeout' => 30,
                            ],
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_otlp_http_transport_with_psr_services() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'type' => 'http',
                                'endpoint' => 'http://localhost:4318',
                                'http_client_service_id' => 'app.http_client',
                                'request_factory_service_id' => 'app.request_factory',
                                'stream_factory_service_id' => 'app.stream_factory',
                            ],
                        ],
                    ],
                ],
            ],
        ]]);

        $transport = $config['tracer_provider']['processor']['exporter']['otlp']['transport'];
        self::assertSame('http', $transport['type']);
        self::assertSame('app.http_client', $transport['http_client_service_id']);
        self::assertSame('app.request_factory', $transport['request_factory_service_id']);
        self::assertSame('app.stream_factory', $transport['stream_factory_service_id']);
    }

    public function test_otlp_serializer_defaults_to_json() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'endpoint' => 'http://localhost:4318',
                            ],
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
            'resource' => [],
            'tracer_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => [
                        'type' => 'otlp',
                        'otlp' => [
                            'transport' => [
                                'endpoint' => 'http://localhost:4318',
                            ],
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
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
            'tracer_provider' => [],
        ]]);

        self::assertSame('void', $config['tracer_provider']['processor']['type']);
    }

    public function test_providers_have_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertArrayHasKey('tracer_provider', $config);
        self::assertArrayHasKey('meter_provider', $config);
        self::assertArrayHasKey('logger_provider', $config);
        self::assertSame('void', $config['tracer_provider']['processor']['type']);
        self::assertSame('void', $config['meter_provider']['processor']['type']);
        self::assertSame('void', $config['logger_provider']['processor']['type']);
    }

    public function test_resource_custom_attributes() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [
                'custom' => [
                    'my.custom.attribute' => 'custom-value',
                    'another.attribute' => 123,
                ],
            ],
        ]]);

        self::assertSame([
            'my.custom.attribute' => 'custom-value',
            'another.attribute' => 123,
        ], $config['resource']['custom']);
    }

    public function test_resource_detectors_are_enabled_by_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
        ]]);

        self::assertTrue($config['resource']['detectors']['enabled']);
        self::assertTrue($config['resource']['detectors']['static']['cache']['enabled']);
        self::assertNull($config['resource']['detectors']['static']['cache']['path']);
        self::assertTrue($config['resource']['detectors']['static']['os']['enabled']);
        self::assertTrue($config['resource']['detectors']['static']['host']['enabled']);
        self::assertTrue($config['resource']['detectors']['static']['service']['enabled']);
        self::assertTrue($config['resource']['detectors']['static']['deployment']['enabled']);
        self::assertTrue($config['resource']['detectors']['static']['environment']['enabled']);
        self::assertTrue($config['resource']['detectors']['dynamic']['process']['enabled']);
    }

    public function test_resource_detectors_cache_can_be_configured() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [
                'detectors' => [
                    'static' => [
                        'cache' => [
                            'enabled' => false,
                            'path' => '/custom/cache/path',
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertFalse($config['resource']['detectors']['static']['cache']['enabled']);
        self::assertSame('/custom/cache/path', $config['resource']['detectors']['static']['cache']['path']);
    }

    public function test_resource_detectors_can_be_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [
                'detectors' => [
                    'enabled' => false,
                ],
            ],
        ]]);

        self::assertFalse($config['resource']['detectors']['enabled']);
    }

    public function test_resource_detectors_can_be_individually_disabled() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [
                'detectors' => [
                    'static' => [
                        'os' => ['enabled' => false],
                        'host' => ['enabled' => false],
                        'service' => ['enabled' => false],
                        'deployment' => ['enabled' => false],
                        'environment' => ['enabled' => false],
                    ],
                    'dynamic' => [
                        'process' => ['enabled' => false],
                    ],
                ],
            ],
        ]]);

        self::assertTrue($config['resource']['detectors']['enabled']);
        self::assertFalse($config['resource']['detectors']['static']['os']['enabled']);
        self::assertFalse($config['resource']['detectors']['static']['host']['enabled']);
        self::assertFalse($config['resource']['detectors']['static']['service']['enabled']);
        self::assertFalse($config['resource']['detectors']['static']['deployment']['enabled']);
        self::assertFalse($config['resource']['detectors']['static']['environment']['enabled']);
        self::assertFalse($config['resource']['detectors']['dynamic']['process']['enabled']);
    }

    public function test_sampler_defaults_to_always_on() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
            'tracer_provider' => [],
        ]]);

        self::assertSame('always_on', $config['tracer_provider']['sampler']['type']);
    }

    public function test_sampler_ratio_maximum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
            'tracer_provider' => [
                'sampler' => [
                    'type' => 'trace_id_ratio',
                    'ratio' => 0.5,
                ],
            ],
        ]]);

        self::assertSame(0.5, $config['tracer_provider']['sampler']['ratio']);
    }

    public function test_severity_filtering_is_only_available_for_log_processors() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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
            'resource' => [],
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
            'resource' => [],
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

    public function test_tracer_configuration_with_all_options() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'resource' => [],
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
            'resource' => [],
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
