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
            'instances' => [
                'default' => [
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
                ],
            ],
        ]]);

        $processors = $config['instances']['default']['tracer_provider']['processor']['processors'];
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

    public function test_exporter_defaults_to_void() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame('void', $config['instances']['default']['tracer_provider']['processor']['exporter']['type']);
    }

    public function test_instances_key_is_present_when_omitted() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
        ]]);

        self::assertArrayHasKey('instances', $config);
        self::assertSame([], $config['instances']);
    }

    public function test_invalid_exporter_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'exporter' => [
                                'type' => 'invalid_exporter',
                            ],
                        ],
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
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'invalid_processor',
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_invalid_sampler_type_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'invalid_sampler',
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_invalid_severity_level_is_rejected() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'severity_filtering',
                            'minimum_severity' => 'invalid_level',
                            'inner_processor' => [
                                'type' => 'void',
                            ],
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_meter_provider_temporality_can_be_delta() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'meter_provider' => [
                        'temporality' => 'delta',
                    ],
                ],
            ],
        ]]);

        self::assertSame('delta', $config['instances']['default']['meter_provider']['temporality']);
    }

    public function test_meter_provider_temporality_defaults_to_cumulative() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'meter_provider' => [],
                ],
            ],
        ]]);

        self::assertSame('cumulative', $config['instances']['default']['meter_provider']['temporality']);
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

    public function test_multiple_instances() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [],
                'secondary' => [
                    'tracer_provider' => [
                        'sampler' => ['type' => 'always_off'],
                    ],
                ],
            ],
        ]]);

        self::assertArrayHasKey('default', $config['instances']);
        self::assertArrayHasKey('secondary', $config['instances']);
        self::assertSame('always_off', $config['instances']['secondary']['tracer_provider']['sampler']['type']);
    }

    public function test_otlp_serializer_defaults_to_json() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
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
                ],
            ],
        ]]);

        $serializer = $config['instances']['default']['tracer_provider']['processor']['exporter']['otlp']['transport']['serializer'];
        self::assertSame('json', $serializer['type']);
    }

    public function test_otlp_transport_defaults() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
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
                ],
            ],
        ]]);

        $transport = $config['instances']['default']['tracer_provider']['processor']['exporter']['otlp']['transport'];
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
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame(512, $config['instances']['default']['tracer_provider']['processor']['batch_size']);
    }

    public function test_processor_batch_size_minimum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 0,
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_processor_defaults_to_void() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [],
                ],
            ],
        ]]);

        self::assertSame('void', $config['instances']['default']['tracer_provider']['processor']['type']);
    }

    public function test_sampler_defaults_to_always_on() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [],
                ],
            ],
        ]]);

        self::assertSame('always_on', $config['instances']['default']['tracer_provider']['sampler']['type']);
    }

    public function test_sampler_ratio_maximum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'trace_id_ratio',
                            'ratio' => 1.1,
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_sampler_ratio_minimum_validation() : void
    {
        $this->expectException(InvalidConfigurationException::class);

        (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'trace_id_ratio',
                            'ratio' => -0.1,
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_sampler_ratio_validation() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'trace_id_ratio',
                            'ratio' => 0.5,
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame(0.5, $config['instances']['default']['tracer_provider']['sampler']['ratio']);
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
            'instances' => [
                'default' => [
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'severity_filtering',
                        ],
                    ],
                ],
            ],
        ]]);
    }

    public function test_severity_filtering_minimum_severity_default() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'severity_filtering',
                            'inner_processor' => [
                                'type' => 'void',
                            ],
                        ],
                    ],
                ],
            ],
        ]]);

        self::assertSame('info', $config['instances']['default']['logger_provider']['processor']['minimum_severity']);
    }

    public function test_severity_filtering_processor_for_logs() : void
    {
        $config = (new Processor())->processConfiguration(new Configuration(), [[
            'service' => ['name' => 'test-app'],
            'instances' => [
                'default' => [
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
                ],
            ],
        ]]);

        $processor = $config['instances']['default']['logger_provider']['processor'];
        self::assertSame('severity_filtering', $processor['type']);
        self::assertSame('warn', $processor['minimum_severity']);
        self::assertSame('batching', $processor['inner_processor']['type']);
        self::assertSame('console', $processor['inner_processor']['exporter']['type']);
    }
}
