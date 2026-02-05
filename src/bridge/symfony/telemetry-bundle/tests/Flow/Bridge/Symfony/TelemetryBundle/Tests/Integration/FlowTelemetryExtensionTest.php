<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\FlowTelemetryExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, CompositeLogProcessor, PassThroughLogProcessor};
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, CompositeMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleMetricExporter, ConsoleSpanExporter};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\{Resource, Telemetry};
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
                    'instances' => [
                        'default' => [
                            'logger_provider' => [
                                'processor' => [
                                    'type' => 'composite',
                                    'processors' => [
                                        ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                        ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(CompositeLogProcessor::class, $this->getContainer()->get('flow.telemetry.default.logger_provider.processor'));
    }

    public function test_composite_metric_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'meter_provider' => [
                                'processor' => [
                                    'type' => 'composite',
                                    'processors' => [
                                        ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                        ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(CompositeMetricProcessor::class, $this->getContainer()->get('flow.telemetry.default.meter_provider.processor'));
    }

    public function test_composite_span_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => [
                                    'type' => 'composite',
                                    'processors' => [
                                        ['type' => 'memory', 'exporter' => ['type' => 'memory']],
                                        ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(CompositeSpanProcessor::class, $container->get('flow.telemetry.default.tracer_provider.processor'));
        self::assertInstanceOf(MemorySpanProcessor::class, $container->get('flow.telemetry.default.tracer_provider.processor.0.processor'));
        self::assertInstanceOf(PassThroughSpanProcessor::class, $container->get('flow.telemetry.default.tracer_provider.processor.1.processor'));
    }

    public function test_custom_service_reference_for_exporter() : void
    {
        $container = new ContainerBuilder();

        $container->register('my.custom.span_exporter', MemorySpanExporter::class)->setPublic(true);

        $extension = new FlowTelemetryExtension();
        $extension->load([
            [
                'service' => ['name' => 'test-app'],
                'instances' => [
                    'default' => [
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
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.span_exporter'),
            $container->get('flow.telemetry.default.tracer_provider.processor.exporter')
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
                'instances' => [
                    'default' => [
                        'tracer_provider' => [
                            'processor' => [
                                'type' => 'service',
                                'service_id' => 'my.custom.span_processor',
                            ],
                        ],
                    ],
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.span_processor'),
            $container->get('flow.telemetry.default.tracer_provider.processor')
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
                'instances' => [
                    'default' => [
                        'tracer_provider' => [
                            'sampler' => [
                                'type' => 'service',
                                'service_id' => 'my.custom.sampler',
                            ],
                        ],
                    ],
                ],
            ],
        ], $container);

        $this->makeFlowServicesPublic($container);
        $container->compile();

        self::assertSame(
            $container->get('my.custom.sampler'),
            $container->get('flow.telemetry.default.tracer_provider.sampler')
        );
    }

    public function test_default_instance_is_aliased_to_telemetry_class() : void
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
        self::assertSame($container->get('flow.telemetry.default'), $container->get(Telemetry::class));
    }

    public function test_full_configuration_scenario() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => [
                        'name' => 'my-application',
                        'version' => '3.0.0',
                        'attributes' => [
                            'deployment.environment' => 'staging',
                        ],
                    ],
                    'instances' => [
                        'default' => [
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
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var resource $resource */
        $resource = $container->get('flow.telemetry.resource');
        self::assertSame('my-application', $resource->get('service.name'));
        self::assertSame('3.0.0', $resource->get('service.version'));
        self::assertSame('staging', $resource->get('deployment.environment'));

        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry.default'));

        self::assertInstanceOf(TracerProvider::class, $container->get('flow.telemetry.default.tracer_provider'));
        self::assertInstanceOf(TraceIdRatioBasedSampler::class, $container->get('flow.telemetry.default.tracer_provider.sampler'));
        self::assertInstanceOf(BatchingSpanProcessor::class, $container->get('flow.telemetry.default.tracer_provider.processor'));
        self::assertInstanceOf(ConsoleSpanExporter::class, $container->get('flow.telemetry.default.tracer_provider.processor.exporter'));

        self::assertInstanceOf(MeterProvider::class, $container->get('flow.telemetry.default.meter_provider'));
        self::assertInstanceOf(PassThroughMetricProcessor::class, $container->get('flow.telemetry.default.meter_provider.processor'));
        self::assertInstanceOf(MemoryMetricExporter::class, $container->get('flow.telemetry.default.meter_provider.processor.exporter'));

        self::assertInstanceOf(LoggerProvider::class, $container->get('flow.telemetry.default.logger_provider'));
        self::assertInstanceOf(MemoryLogProcessor::class, $container->get('flow.telemetry.default.logger_provider.processor'));
        self::assertInstanceOf(ConsoleLogExporter::class, $container->get('flow.telemetry.default.logger_provider.processor.exporter'));
    }

    public function test_log_exporter_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                        'with_memory' => ['logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]]],
                        'with_console' => ['logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidLogExporter::class, $container->get('flow.telemetry.with_void.logger_provider.processor.exporter'));
        self::assertInstanceOf(MemoryLogExporter::class, $container->get('flow.telemetry.with_memory.logger_provider.processor.exporter'));
        self::assertInstanceOf(ConsoleLogExporter::class, $container->get('flow.telemetry.with_console.logger_provider.processor.exporter'));
    }

    public function test_log_processor_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['logger_provider' => ['processor' => ['type' => 'void']]],
                        'with_memory' => ['logger_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]]],
                        'with_batching' => ['logger_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 256, 'exporter' => ['type' => 'void']]]],
                        'with_passthrough' => ['logger_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidLogProcessor::class, $container->get('flow.telemetry.with_void.logger_provider.processor'));
        self::assertInstanceOf(MemoryLogProcessor::class, $container->get('flow.telemetry.with_memory.logger_provider.processor'));
        self::assertInstanceOf(BatchingLogProcessor::class, $container->get('flow.telemetry.with_batching.logger_provider.processor'));
        self::assertInstanceOf(PassThroughLogProcessor::class, $container->get('flow.telemetry.with_passthrough.logger_provider.processor'));
    }

    public function test_metric_exporter_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                        'with_memory' => ['meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]]],
                        'with_console' => ['meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidMetricExporter::class, $container->get('flow.telemetry.with_void.meter_provider.processor.exporter'));
        self::assertInstanceOf(MemoryMetricExporter::class, $container->get('flow.telemetry.with_memory.meter_provider.processor.exporter'));
        self::assertInstanceOf(ConsoleMetricExporter::class, $container->get('flow.telemetry.with_console.meter_provider.processor.exporter'));
    }

    public function test_metric_processor_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['meter_provider' => ['processor' => ['type' => 'void']]],
                        'with_memory' => ['meter_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]]],
                        'with_batching' => ['meter_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 200, 'exporter' => ['type' => 'void']]]],
                        'with_passthrough' => ['meter_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidMetricProcessor::class, $container->get('flow.telemetry.with_void.meter_provider.processor'));
        self::assertInstanceOf(MemoryMetricProcessor::class, $container->get('flow.telemetry.with_memory.meter_provider.processor'));
        self::assertInstanceOf(BatchingMetricProcessor::class, $container->get('flow.telemetry.with_batching.meter_provider.processor'));
        self::assertInstanceOf(PassThroughMetricProcessor::class, $container->get('flow.telemetry.with_passthrough.meter_provider.processor'));
    }

    public function test_minimal_configuration_creates_default_instance_with_void_processors() : void
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
        self::assertTrue($container->has('flow.telemetry.default'));

        self::assertInstanceOf(SystemClock::class, $container->get('flow.telemetry.clock'));
        self::assertInstanceOf(MemoryContextStorage::class, $container->get('flow.telemetry.context_storage'));
        self::assertInstanceOf(Resource::class, $container->get('flow.telemetry.resource'));
        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry.default'));

        self::assertTrue($container->has('flow.telemetry.default.tracer_provider'));
        self::assertTrue($container->has('flow.telemetry.default.meter_provider'));
        self::assertTrue($container->has('flow.telemetry.default.logger_provider'));

        self::assertInstanceOf(TracerProvider::class, $container->get('flow.telemetry.default.tracer_provider'));
        self::assertInstanceOf(MeterProvider::class, $container->get('flow.telemetry.default.meter_provider'));
        self::assertInstanceOf(LoggerProvider::class, $container->get('flow.telemetry.default.logger_provider'));

        self::assertTrue($container->has('flow.telemetry.default.tracer_provider.processor'));
        self::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.default.tracer_provider.processor'));

        self::assertTrue($container->has('flow.telemetry.default.meter_provider.processor'));
        self::assertInstanceOf(VoidMetricProcessor::class, $container->get('flow.telemetry.default.meter_provider.processor'));

        self::assertTrue($container->has('flow.telemetry.default.logger_provider.processor'));
        self::assertInstanceOf(VoidLogProcessor::class, $container->get('flow.telemetry.default.logger_provider.processor'));
    }

    public function test_multiple_telemetry_instances() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'main' => [
                            'tracer_provider' => [
                                'processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']],
                            ],
                        ],
                        'secondary' => [
                            'tracer_provider' => [
                                'processor' => ['type' => 'batching', 'exporter' => ['type' => 'memory']],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.main'));
        self::assertTrue($container->has('flow.telemetry.secondary'));

        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry.main'));
        self::assertInstanceOf(Telemetry::class, $container->get('flow.telemetry.secondary'));

        self::assertNotSame($container->get('flow.telemetry.main'), $container->get('flow.telemetry.secondary'));

        self::assertInstanceOf(PassThroughSpanProcessor::class, $container->get('flow.telemetry.main.tracer_provider.processor'));
        self::assertInstanceOf(BatchingSpanProcessor::class, $container->get('flow.telemetry.secondary.tracer_provider.processor'));
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
                        'version' => '2.1.0',
                    ],
                ]);
            },
        ]);

        /** @var resource $resource */
        $resource = $this->getContainer()->get('flow.telemetry.resource');
        self::assertSame('my-service', $resource->get('service.name'));
        self::assertSame('2.1.0', $resource->get('service.version'));
    }

    public function test_service_exporter_without_service_id_throws_exception() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('service_id is required when exporter type is "service"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => [
                                    'type' => 'passthrough',
                                    'exporter' => ['type' => 'service'],
                                ],
                            ],
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
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'processor' => ['type' => 'service'],
                            ],
                        ],
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
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'sampler' => ['type' => 'service'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);
    }

    public function test_span_exporter_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                        'with_memory' => ['tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'memory']]]],
                        'with_console' => ['tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'console']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidSpanExporter::class, $container->get('flow.telemetry.with_void.tracer_provider.processor.exporter'));
        self::assertInstanceOf(MemorySpanExporter::class, $container->get('flow.telemetry.with_memory.tracer_provider.processor.exporter'));
        self::assertInstanceOf(ConsoleSpanExporter::class, $container->get('flow.telemetry.with_console.tracer_provider.processor.exporter'));
    }

    public function test_span_processor_types() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'with_void' => ['tracer_provider' => ['processor' => ['type' => 'void']]],
                        'with_memory' => ['tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => ['type' => 'void']]]],
                        'with_batching' => ['tracer_provider' => ['processor' => ['type' => 'batching', 'batch_size' => 100, 'exporter' => ['type' => 'void']]]],
                        'with_passthrough' => ['tracer_provider' => ['processor' => ['type' => 'passthrough', 'exporter' => ['type' => 'void']]]],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.with_void.tracer_provider.processor'));
        self::assertInstanceOf(MemorySpanProcessor::class, $container->get('flow.telemetry.with_memory.tracer_provider.processor'));
        self::assertInstanceOf(BatchingSpanProcessor::class, $container->get('flow.telemetry.with_batching.tracer_provider.processor'));
        self::assertInstanceOf(PassThroughSpanProcessor::class, $container->get('flow.telemetry.with_passthrough.tracer_provider.processor'));
    }

    public function test_tracer_provider_with_always_off_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'sampler' => ['type' => 'always_off'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(AlwaysOffSampler::class, $this->getContainer()->get('flow.telemetry.default.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_always_on_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'sampler' => ['type' => 'always_on'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.default.tracer_provider.sampler'));
        self::assertInstanceOf(AlwaysOnSampler::class, $container->get('flow.telemetry.default.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_parent_based_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'service' => ['name' => 'test-app'],
                    'instances' => [
                        'default' => [
                            'tracer_provider' => [
                                'sampler' => ['type' => 'parent_based'],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        self::assertInstanceOf(ParentBasedSampler::class, $this->getContainer()->get('flow.telemetry.default.tracer_provider.sampler'));
    }

    public function test_tracer_provider_with_trace_id_ratio_sampler() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
                ]);
            },
        ]);

        self::assertInstanceOf(TraceIdRatioBasedSampler::class, $this->getContainer()->get('flow.telemetry.default.tracer_provider.sampler'));
    }
}
