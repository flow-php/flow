<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\{FrameworkLoggerPass, OTLPAvailabilityPass};
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\FlowTelemetryExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, GrpcTransport};
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, SeverityFilteringLogProcessor};
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\ConsoleExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\{VoidExporter, VoidLogProcessor, VoidMetricProcessor, VoidSpanProcessor};
use Flow\Telemetry\Resource\Detector\CachingDetector;
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\Processor\{BatchingSpanProcessor, CompositeSpanProcessor};
use Flow\Telemetry\Transport\VoidTransport;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition};
use Symfony\Component\HttpKernel\Log\Logger as SymfonyDefaultLogger;

#[CoversClass(FlowTelemetryExtension::class)]
#[CoversClass(OTLPAvailabilityPass::class)]
#[CoversClass(FrameworkLoggerPass::class)]
final class FlowTelemetryExtensionTest extends KernelTestCase
{
    public function test_auto_alias_when_logger_service_is_symfony_default() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $loggerDefinition = new Definition(SymfonyDefaultLogger::class);
                    $loggerDefinition->setPublic(true);
                    $container->setDefinition('logger', $loggerDefinition);
                });
            },
        ]);

        $container = $this->getContainer();

        self::assertSame(
            $container->get('flow.telemetry.default.logger.psr3'),
            $container->get('logger'),
        );
    }

    public function test_caching_detector_writes_to_configured_path() : void
    {
        $cachePath = \sys_get_temp_dir() . '/flow_telemetry_resource_' . \uniqid() . '.cache';

        try {
            $this->bootKernel([
                'config' => static function (TestKernel $kernel) use ($cachePath) : void {
                    $kernel->addTestExtensionConfig('flow_telemetry', [
                        'resource' => [
                            'detectors' => [
                                'static' => [
                                    'cache' => ['path' => $cachePath],
                                ],
                            ],
                            'custom' => ['service.name' => 'cached-service'],
                        ],
                    ]);
                },
            ]);

            $container = $this->getContainer();
            $detector = $container->get('flow.telemetry.resource.detector.static');
            self::assertInstanceOf(CachingDetector::class, $detector);

            $resource = $container->get('flow.telemetry.resource');
            self::assertInstanceOf(Resource::class, $resource);
            self::assertSame('cached-service', $resource->get('service.name'));
            self::assertFileExists($cachePath);
        } finally {
            if (\is_file($cachePath)) {
                \unlink($cachePath);
            }
        }
    }

    public function test_clock_can_be_overridden_with_custom_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'clock_service_id' => 'app.custom_clock',
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $clockDefinition = new Definition(SystemClock::class);
                    $clockDefinition->setPublic(true);
                    $container->setDefinition('app.custom_clock', $clockDefinition);
                });
            },
        ]);

        $container = $this->getContainer();
        self::assertTrue($container->has('flow.telemetry.clock'));
    }

    public function test_composite_span_processor_with_named_exporters() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
                                ['type' => 'batching', 'exporter' => 'otlp', 'batch_size' => 256],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        $processor = $container->get('flow.telemetry.tracer_provider.processor');

        self::assertInstanceOf(CompositeSpanProcessor::class, $processor);
        self::assertCount(2, $processor->processors());
        self::assertInstanceOf(MemorySpanProcessor::class, $processor->processors()[0]);
        self::assertInstanceOf(BatchingSpanProcessor::class, $processor->processors()[1]);
    }

    public function test_console_exporter_is_registered() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'console' => ['console' => null],
                    ],
                    'logger_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'console'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(ConsoleExporter::class, $container->get('flow.telemetry.exporter.console'));
    }

    public function test_curl_transport_is_built_inline_inside_otlp_exporter() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'curl',
                                    'endpoint' => 'http://localhost:4318',
                                    'serializer' => ['type' => 'protobuf'],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(CurlTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
        self::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp'));
    }

    public function test_custom_exporter_via_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'custom' => ['service' => ['id' => 'app.my_exporter']],
                    ],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'custom'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $definition = new Definition(VoidExporter::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_exporter', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        $exporter = $container->get('flow.telemetry.exporter.custom');
        self::assertInstanceOf(VoidExporter::class, $exporter);
    }

    public function test_custom_processor_via_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => ['type' => 'service', 'service_id' => 'app.my_span_processor'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $definition = new Definition(VoidSpanProcessor::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_span_processor', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_custom_transport_via_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'otlp' => [
                                'transport' => ['type' => 'service', 'service_id' => 'app.my_transport'],
                            ],
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $definition = new Definition(VoidTransport::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_transport', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(VoidTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
    }

    public function test_default_telemetry_is_void() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(Telemetry::class, $container->get(Telemetry::class));
        self::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
        self::assertInstanceOf(VoidMetricProcessor::class, $container->get('flow.telemetry.meter_provider.processor'));
        self::assertInstanceOf(VoidLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_minimal_otlp_setup_registers_three_providers_with_one_exporter() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'curl',
                                    'endpoint' => 'http://localhost:4318',
                                    'serializer' => ['type' => 'protobuf'],
                                ],
                            ],
                        ],
                    ],
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
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(BatchingSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
        self::assertInstanceOf(BatchingMetricProcessor::class, $container->get('flow.telemetry.meter_provider.processor'));
        self::assertInstanceOf(BatchingLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
        self::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp'));
    }

    public function test_processor_referencing_unknown_exporter_throws() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('references unknown exporter "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'missing'],
                    ],
                ]);
            },
        ]);
    }

    public function test_severity_filtering_wraps_batching_log_processor() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
                ]);
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(SeverityFilteringLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_two_separate_otlp_backends() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp_traces' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'grpc',
                                    'endpoint' => 'http://traces:4317',
                                    'insecure' => true,
                                    'serializer' => ['type' => 'protobuf'],
                                ],
                            ],
                        ],
                        'otlp_metrics' => [
                            'otlp' => [
                                'transport' => ['type' => 'curl', 'endpoint' => 'http://metrics:4318'],
                            ],
                        ],
                    ],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'otlp_traces'],
                    ],
                    'meter_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'otlp_metrics'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        self::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_traces'));
        self::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_metrics'));

        if (\extension_loaded('grpc')) {
            self::assertInstanceOf(GrpcTransport::class, $container->get('flow.telemetry.exporter.otlp_traces.transport'));
        }
        self::assertInstanceOf(CurlTransport::class, $container->get('flow.telemetry.exporter.otlp_metrics.transport'));
    }
}
