<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\FlowTelemetryExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\{Logger\Logger, Meter\Meter, Resource, Telemetry, Tracer\Tracer};
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, CompositeLogProcessor, PassThroughLogProcessor};
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, CompositeMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleMetricExporter, ConsoleSpanExporter};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\Tracer\Processor\{BatchingSpanProcessor, CompositeSpanProcessor, PassThroughSpanProcessor};
use Flow\Telemetry\Tracer\Sampler\{AlwaysOffSampler, AlwaysOnSampler, ParentBasedSampler, TraceIdRatioBasedSampler};
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;

#[CoversClass(FlowTelemetryExtension::class)]
#[CoversClass(OTLPAvailabilityPass::class)]
final class FlowTelemetryExtensionTest extends KernelTestCase
{
    public function test_composite_log_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'composite',
                            'processors' => [
                                ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(CompositeLogProcessor::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_composite_metric_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => [
                        'processor' => [
                            'type' => 'composite',
                            'processors' => [
                                ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(CompositeMetricProcessor::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor'));
    }

    public function test_composite_span_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'composite',
                            'processors' => [
                                ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(CompositeSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
        self::assertInstanceOf(MemorySpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor.0.processor'));
        self::assertInstanceOf(PassThroughSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor.1.processor'));
    }

    public function test_custom_service_reference_for_exporter() : void
    {
        $container = new ContainerBuilder();

        $container->register('my.custom.span_exporter', MemorySpanExporter::class)->setPublic(true);

        $extension = new FlowTelemetryExtension();
        $extension->load([
            [
                'service' => ['name' => 'test-app'],
                'tracer_provider' => [
                    'processor' => [
                        'type' => 'passthrough',
                        'exporter' => [
                            'type' => 'service',
                            'service_id' => 'my.custom.span_exporter',
                        ],
                    ],
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.span_exporter'),
            $container->get('flow.telemetry.tracer_provider.processor.exporter')
        );
    }

    public function test_custom_service_reference_for_processor() : void
    {
        $container = new ContainerBuilder();

        $container->register('my.custom.span_processor', VoidSpanProcessor::class)->setPublic(true);

        $extension = new FlowTelemetryExtension();
        $extension->load([
            [
                'service' => ['name' => 'test-app'],
                'tracer_provider' => [
                    'processor' => [
                        'type' => 'service',
                        'service_id' => 'my.custom.span_processor',
                    ],
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.span_processor'),
            $container->get('flow.telemetry.tracer_provider.processor')
        );
    }

    public function test_custom_service_reference_for_sampler() : void
    {
        $container = new ContainerBuilder();

        $container->register('my.custom.sampler', AlwaysOffSampler::class)->setPublic(true);

        $extension = new FlowTelemetryExtension();
        $extension->load([
            [
                'service' => ['name' => 'test-app'],
                'tracer_provider' => [
                    'sampler' => [
                        'type' => 'service',
                        'service_id' => 'my.custom.sampler',
                    ],
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.sampler'),
            $container->get('flow.telemetry.tracer_provider.sampler')
        );
    }

    public function test_flow_telemetry_is_aliased_to_telemetry_class() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has(Telemetry::class));
        self::assertSame($container->get('flow.telemetry'), $container->get(Telemetry::class));
    }

    public function test_full_configuration_scenario() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => [
                        'name' => 'my-application',
                        'version' => ['type' => 'manual', 'value' => '3.0.0'],
                        'attributes' => [
                            'deployment.environment' => 'staging',
                        ],
                    ],
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'trace_id_ratio',
                            'ratio' => 0.75,
                        ],
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 1024,
                            'exporter' => ['type' => 'console'],
                        ],
                    ],
                    'meter_provider' => [
                        'temporality' => 'delta',
                        'processor' => [
                            'type' => 'passthrough',
                            'exporter' => ['type' => 'memory'],
                        ],
                    ],
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'console'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var resource $resource */
        $resource = $container->get('flow.telemetry.resource');
        self::assertSame('my-application', $resource->get('name'));
        self::assertSame('3.0.0', $resource->get('version'));
        self::assertSame('staging', $resource->get('deployment.environment'));

        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry'));

        self::assertInstanceOf(TracerProvider::class, $container->get('flow.telemetry.tracer_provider'));
        self::assertInstanceOf(TraceIdRatioBasedSampler::class, $container->get('flow.telemetry.tracer_provider.sampler'));
        self::assertInstanceOf(BatchingSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
        self::assertInstanceOf(ConsoleSpanExporter::class, $container->get('flow.telemetry.tracer_provider.processor.exporter'));

        self::assertInstanceOf(MeterProvider::class, $container->get('flow.telemetry.meter_provider'));
        self::assertInstanceOf(PassThroughMetricProcessor::class, $container->get('flow.telemetry.meter_provider.processor'));
        self::assertInstanceOf(MemoryMetricExporter::class, $container->get('flow.telemetry.meter_provider.processor.exporter'));

        self::assertInstanceOf(LoggerProvider::class, $container->get('flow.telemetry.logger_provider'));
        self::assertInstanceOf(MemoryLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
        self::assertInstanceOf(ConsoleLogExporter::class, $container->get('flow.telemetry.logger_provider.processor.exporter'));
    }

    public function test_log_exporter_console_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]],
                ]);
            },
        ]);

        self::assertInstanceOf(ConsoleLogExporter::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor.exporter'));
    }

    public function test_log_exporter_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemoryLogExporter::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor.exporter'));
    }

    public function test_log_exporter_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidLogExporter::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor.exporter'));
    }

    public function test_log_processor_batching_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 256, 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(BatchingLogProcessor::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_log_processor_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemoryLogProcessor::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_log_processor_passthrough_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(PassThroughLogProcessor::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_log_processor_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'logger_provider' => ['processor' => ['type' => 'void']],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidLogProcessor::class, $this->getContainer()->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_metric_exporter_console_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]],
                ]);
            },
        ]);

        self::assertInstanceOf(ConsoleMetricExporter::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor.exporter'));
    }

    public function test_metric_exporter_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemoryMetricExporter::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor.exporter'));
    }

    public function test_metric_exporter_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidMetricExporter::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor.exporter'));
    }

    public function test_metric_processor_batching_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 200, 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(BatchingMetricProcessor::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor'));
    }

    public function test_metric_processor_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemoryMetricProcessor::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor'));
    }

    public function test_metric_processor_passthrough_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(PassThroughMetricProcessor::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor'));
    }

    public function test_metric_processor_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meter_provider' => ['processor' => ['type' => 'void']],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidMetricProcessor::class, $this->getContainer()->get('flow.telemetry.meter_provider.processor'));
    }

    public function test_minimal_configuration_creates_telemetry_with_void_processors() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.clock'));
        self::assertTrue($container->has('flow.telemetry.context_storage'));
        self::assertTrue($container->has('flow.telemetry.resource'));
        self::assertTrue($container->has('flow.telemetry'));

        self::assertInstanceOf(SystemClock::class, $container->get('flow.telemetry.clock'));
        self::assertInstanceOf(MemoryContextStorage::class, $container->get('flow.telemetry.context_storage'));
        self::assertInstanceOf(Resource::class, $container->get('flow.telemetry.resource'));
        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry'));

        self::assertTrue($container->has('flow.telemetry.tracer_provider'));
        self::assertTrue($container->has('flow.telemetry.meter_provider'));
        self::assertTrue($container->has('flow.telemetry.logger_provider'));

        self::assertInstanceOf(TracerProvider::class, $container->get('flow.telemetry.tracer_provider'));
        self::assertInstanceOf(MeterProvider::class, $container->get('flow.telemetry.meter_provider'));
        self::assertInstanceOf(LoggerProvider::class, $container->get('flow.telemetry.logger_provider'));

        self::assertTrue($container->has('flow.telemetry.tracer_provider.processor'));
        self::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));

        self::assertTrue($container->has('flow.telemetry.meter_provider.processor'));
        self::assertInstanceOf(VoidMetricProcessor::class, $container->get('flow.telemetry.meter_provider.processor'));

        self::assertTrue($container->has('flow.telemetry.logger_provider.processor'));
        self::assertInstanceOf(VoidLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_multiple_named_services_of_same_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracers' => [
                        'database' => [
                            'version' => '1.0.0',
                        ],
                        'http_client' => [
                            'version' => '2.0.0',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.database.tracer'));
        self::assertTrue($container->has('flow.telemetry.http_client.tracer'));
        self::assertInstanceOf(Tracer::class, $container->get('flow.telemetry.database.tracer'));
        self::assertInstanceOf(Tracer::class, $container->get('flow.telemetry.http_client.tracer'));
    }

    public function test_named_logger_is_registered_as_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'loggers' => [
                        'audit' => [
                            'version' => '1.0.0',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.audit.logger'));
        self::assertInstanceOf(Logger::class, $container->get('flow.telemetry.audit.logger'));
    }

    public function test_named_meter_is_registered_as_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'meters' => [
                        'etl_pipeline' => [
                            'version' => '1.0.0',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.etl_pipeline.meter'));
        self::assertInstanceOf(Meter::class, $container->get('flow.telemetry.etl_pipeline.meter'));
    }

    public function test_named_tracer_is_registered_as_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracers' => [
                        'database' => [
                            'version' => '2.0.0',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.database.tracer'));
        self::assertInstanceOf(Tracer::class, $container->get('flow.telemetry.database.tracer'));
    }

    public function test_named_tracer_with_attributes() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracers' => [
                        'database' => [
                            'version' => '2.0.0',
                            'schema_url' => 'https://opentelemetry.io/schemas/1.20.0',
                            'attributes' => [
                                'db.system' => 'postgresql',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        $tracer = $container->get('flow.telemetry.database.tracer');

        self::assertInstanceOf(Tracer::class, $tracer);
    }

    public function test_otlp_availability_pass_sets_parameter_when_otlp_not_configured() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                ]);
            },
        ]);

        self::assertTrue($this->getContainer()->hasParameter('flow.telemetry.otlp_available'));
    }

    public function test_resource_contains_additional_attributes() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => [
                        'name' => 'my-service',
                        'attributes' => [
                            'deployment.environment' => 'production',
                            'host.name' => 'server-01',
                        ],
                    ],
                ]);
            },
        ]);

        /** @var resource $resource */
        $resource = $this->getContainer()->get('flow.telemetry.resource');
        self::assertSame('production', $resource->get('deployment.environment'));
        self::assertSame('server-01', $resource->get('host.name'));
    }

    public function test_resource_contains_service_name_and_version() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => [
                        'name' => 'my-service',
                        'version' => ['type' => 'manual', 'value' => '2.1.0'],
                    ],
                ]);
            },
        ]);

        /** @var resource $resource */
        $resource = $this->getContainer()->get('flow.telemetry.resource');
        self::assertSame('my-service', $resource->get('name'));
        self::assertSame('2.1.0', $resource->get('version'));
    }

    public function test_same_name_for_different_types_is_allowed() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracers' => [
                        'database' => ['version' => '1.0.0'],
                    ],
                    'meters' => [
                        'database' => ['version' => '1.0.0'],
                    ],
                    'loggers' => [
                        'database' => ['version' => '1.0.0'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.database.tracer'));
        self::assertTrue($container->has('flow.telemetry.database.meter'));
        self::assertTrue($container->has('flow.telemetry.database.logger'));
        self::assertInstanceOf(Tracer::class, $container->get('flow.telemetry.database.tracer'));
        self::assertInstanceOf(Meter::class, $container->get('flow.telemetry.database.meter'));
        self::assertInstanceOf(Logger::class, $container->get('flow.telemetry.database.logger'));
    }

    public function test_service_exporter_without_service_id_throws_exception() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('service_id is required when exporter type is "service"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'passthrough',
                            'exporter' => ['type' => 'service'],
                        ],
                    ],
                ]);
            },
        ]);
    }

    public function test_service_processor_without_service_id_throws_exception() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('service_id is required when processor type is "service"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'processor' => ['type' => 'service'],
                    ],
                ]);
            },
        ]);
    }

    public function test_service_sampler_without_service_id_throws_exception() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('service_id is required when sampler type is "service"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'sampler' => ['type' => 'service'],
                    ],
                ]);
            },
        ]);
    }

    public function test_span_exporter_console_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]],
                ]);
            },
        ]);

        self::assertInstanceOf(ConsoleSpanExporter::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor.exporter'));
    }

    public function test_span_exporter_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemorySpanExporter::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor.exporter'));
    }

    public function test_span_exporter_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidSpanExporter::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor.exporter'));
    }

    public function test_span_processor_batching_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 100, 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(BatchingSpanProcessor::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_span_processor_memory_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(MemorySpanProcessor::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_span_processor_passthrough_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]],
                ]);
            },
        ]);

        self::assertInstanceOf(PassThroughSpanProcessor::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_span_processor_void_type() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => ['processor' => ['type' => 'void']],
                ]);
            },
        ]);

        self::assertInstanceOf(VoidSpanProcessor::class, $this->getContainer()->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_tracer_provider_with_always_off_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'sampler' => ['type' => 'always_off'],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(AlwaysOffSampler::class, $this->getContainer()->get('flow.telemetry.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_always_on_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'sampler' => ['type' => 'always_on'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.tracer_provider.sampler'));
        self::assertInstanceOf(AlwaysOnSampler::class, $container->get('flow.telemetry.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_parent_based_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'sampler' => ['type' => 'parent_based'],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(ParentBasedSampler::class, $this->getContainer()->get('flow.telemetry.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_trace_id_ratio_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'trace_id_ratio',
                            'ratio' => 0.5,
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(TraceIdRatioBasedSampler::class, $this->getContainer()->get('flow.telemetry.tracer_provider.sampler'));
    }
}
