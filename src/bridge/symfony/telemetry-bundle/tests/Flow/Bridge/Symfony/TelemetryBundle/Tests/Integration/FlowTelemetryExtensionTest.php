<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\FrameworkLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\AsyncCurlTransportTickSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerTracePropagation;
use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Telemetry\ErrorHandler\CompositeErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\ErrorHandler\StreamHandler;
use Flow\Telemetry\ErrorHandler\SyslogHandler;
use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Logger\Middleware\SeverityFilteringLogMiddleware;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Processor\PipelineLogProcessor;
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Conditional\ConditionalExporter;
use Flow\Telemetry\Provider\Console\ConsoleExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Detector\CachingDetector;
use Flow\Telemetry\Resource\Detector\GitDetector;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\Processor\CompositeSpanProcessor;
use Flow\Telemetry\Tracer\Sampler\AttributeMatchingSampler;
use Flow\Telemetry\Tracer\Sampler\TraceIdRatioBasedSampler;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\Log\Logger as SymfonyDefaultLogger;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface as MessengerMiddlewareInterface;

use function bin2hex;
use function extension_loaded;
use function interface_exists;
use function is_file;
use function random_bytes;
use function sys_get_temp_dir;
use function uniqid;
use function unlink;

#[CoversClass(FlowTelemetryBundle::class)]
#[CoversClass(OTLPAvailabilityPass::class)]
#[CoversClass(FrameworkLoggerPass::class)]
final class FlowTelemetryExtensionTest extends KernelTestCase
{
    public function test_auto_alias_when_logger_service_is_symfony_default(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $loggerDefinition = new Definition(SymfonyDefaultLogger::class);
                    $loggerDefinition->setPublic(true);
                    $container->setDefinition('logger', $loggerDefinition);
                });
            },
        ]);

        $container = $this->getContainer();

        static::assertSame($container->get('flow.telemetry.default.logger.psr3'), $container->get('logger'));
    }

    public function test_caching_detector_writes_to_configured_path(): void
    {
        $cachePath = sys_get_temp_dir() . '/flow_telemetry_resource_' . uniqid() . '.cache';

        try {
            $this->bootKernel([
                'config' => static function (TestKernel $kernel) use ($cachePath): void {
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
            static::assertInstanceOf(CachingDetector::class, $detector);

            $resource = $container->get('flow.telemetry.resource');
            static::assertInstanceOf(Resource::class, $resource);
            static::assertSame('cached-service', $resource->get('service.name'));
            static::assertFileExists($cachePath);
        } finally {
            if (is_file($cachePath)) {
                unlink($cachePath);
            }
        }
    }

    public function test_caching_detector_default_path_is_keyed_by_kernel_environment(): void
    {
        $expectedPath = sys_get_temp_dir() . '/flow_telemetry_resource_test.cache';

        if (is_file($expectedPath)) {
            unlink($expectedPath);
        }

        try {
            $this->bootKernel([
                'config' => static function (TestKernel $kernel): void {
                    $kernel->addTestExtensionConfig('flow_telemetry', [
                        'resource' => [
                            'custom' => ['service.name' => 'env-keyed-service'],
                        ],
                    ]);
                },
            ]);

            $container = $this->getContainer();
            static::assertInstanceOf(
                CachingDetector::class,
                $container->get('flow.telemetry.resource.detector.static'),
            );

            $resource = $container->get('flow.telemetry.resource');
            static::assertInstanceOf(Resource::class, $resource);
            static::assertSame('env-keyed-service', $resource->get('service.name'));
            static::assertFileExists($expectedPath);
        } finally {
            if (is_file($expectedPath)) {
                unlink($expectedPath);
            }
        }
    }

    public function test_git_detector_is_not_registered_by_default(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.telemetry.resource.detector.git'));
    }

    public function test_git_detector_is_registered_when_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [
                        'detectors' => [
                            'static' => [
                                'git' => ['enabled' => true],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.resource.detector.git'));
        static::assertInstanceOf(GitDetector::class, $container->get('flow.telemetry.resource.detector.git'));
    }

    public function test_clock_can_be_overridden_with_custom_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'clock_service_id' => 'app.custom_clock',
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $clockDefinition = new Definition(SystemClock::class);
                    $clockDefinition->setPublic(true);
                    $container->setDefinition('app.custom_clock', $clockDefinition);
                });
            },
        ]);

        $container = $this->getContainer();
        static::assertTrue($container->has('flow.telemetry.clock'));
    }

    public function test_composite_error_handler_is_registered_with_referenced_children(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => ['type' => 'error_log'],
                        'silent' => ['type' => 'noop'],
                        'fanout' => [
                            'type' => 'composite',
                            'handlers' => ['default', 'silent'],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        $composite = $container->get('flow.telemetry.error_handler.fanout');
        static::assertInstanceOf(CompositeErrorHandler::class, $composite);
        static::assertCount(2, $composite->handlers());
        static::assertInstanceOf(ErrorLogHandler::class, $composite->handlers()[0]);
        static::assertInstanceOf(NullErrorHandler::class, $composite->handlers()[1]);
    }

    public function test_composite_referencing_unknown_child_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown error_handler "ghost"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'fanout' => ['type' => 'composite', 'handlers' => ['ghost']],
                    ],
                ]);
            },
        ]);
    }

    public function test_composite_span_processor_with_named_exporters(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
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

        static::assertInstanceOf(CompositeSpanProcessor::class, $processor);
        static::assertCount(2, $processor->processors());
        static::assertInstanceOf(MemorySpanProcessor::class, $processor->processors()[0]);
        static::assertInstanceOf(BatchingSpanProcessor::class, $processor->processors()[1]);
    }

    public function test_console_exporter_is_registered(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
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
        static::assertInstanceOf(ConsoleExporter::class, $container->get('flow.telemetry.exporter.console'));
    }

    public function test_disabled_exporter_is_replaced_with_void_exporter(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'enabled' => false,
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
                        'processor' => ['type' => 'batching', 'exporter' => 'otlp'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(VoidExporter::class, $container->get('flow.telemetry.exporter.otlp'));
        static::assertFalse($container->has('flow.telemetry.exporter.otlp.transport'));
    }

    public function test_env_bool_flag_drops_export_at_runtime_when_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'primary' => [
                            'enabled' => '%env(bool:FLOW_TEST_OTEL_ENABLED)%',
                            'service' => ['id' => 'app.spy_exporter'],
                        ],
                    ],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'primary'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('env(FLOW_TEST_OTEL_ENABLED)', '0');
                    $container->setDefinition('app.spy_exporter', new Definition(ExporterSpy::class))->setPublic(true);
                });
            },
        ]);

        $container = $this->getContainer();
        $exporter = $container->get('flow.telemetry.exporter.primary');
        static::assertInstanceOf(ConditionalExporter::class, $exporter);

        $spy = $container->get('app.spy_exporter');
        static::assertInstanceOf(ExporterSpy::class, $spy);
        $exporter->export(Signals::traces([SpanMother::withName('span')]));
        static::assertSame(0, $spy->exportedCount());
    }

    public function test_env_bool_flag_forwards_export_at_runtime_when_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'primary' => [
                            'enabled' => '%env(bool:FLOW_TEST_OTEL_ENABLED)%',
                            'service' => ['id' => 'app.spy_exporter'],
                        ],
                    ],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'primary'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('env(FLOW_TEST_OTEL_ENABLED)', '1');
                    $container->setDefinition('app.spy_exporter', new Definition(ExporterSpy::class))->setPublic(true);
                });
            },
        ]);

        $container = $this->getContainer();
        $exporter = $container->get('flow.telemetry.exporter.primary');
        static::assertInstanceOf(ConditionalExporter::class, $exporter);

        $spy = $container->get('app.spy_exporter');
        static::assertInstanceOf(ExporterSpy::class, $spy);
        $exporter->export(Signals::traces([SpanMother::withName('span')]));
        static::assertSame(1, $spy->exportedCount());
    }

    public function test_curl_transport_is_built_inline_inside_otlp_exporter(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(CurlTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
        static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp'));
    }

    public function test_async_curl_transport_is_built_inline_inside_otlp_exporter(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'async_curl',
                                    'endpoint' => 'http://localhost:4318',
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(AsyncCurlTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
    }

    public function test_async_curl_transport_options_and_error_handler_are_wired(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'async_curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.exporter.otlp.transport');
        static::assertSame(AsyncCurlTransport::class, $definition->getClass());

        // @mago-expect analysis:mixed-assignment
        $errorHandler = $definition->getArgument('$errorHandler');
        static::assertInstanceOf(Reference::class, $errorHandler);
        static::assertSame('flow.telemetry.error_handler.default', (string) $errorHandler);

        $optionsDefinition = $container->getDefinition('flow.telemetry.exporter.otlp.transport.options');
        static::assertSame(AsyncCurlTransportOptions::class, $optionsDefinition->getClass());
    }

    public function test_async_curl_transport_is_tagged_for_worker_pump(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'async_curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]], $container);

        static::assertTrue(
            $container
                ->getDefinition('flow.telemetry.exporter.otlp.transport')
                ->hasTag('flow.telemetry.async_curl_transport'),
        );
    }

    public function test_sync_curl_transport_is_not_tagged_for_worker_pump(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]], $container);

        static::assertFalse(
            $container
                ->getDefinition('flow.telemetry.exporter.otlp.transport')
                ->hasTag('flow.telemetry.async_curl_transport'),
        );
    }

    public function test_async_curl_transport_tick_subscriber_is_registered_when_messenger_enabled(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => ['enabled' => true],
            ],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.messenger.async_curl_transport_tick_subscriber');
        static::assertSame(AsyncCurlTransportTickSubscriber::class, $definition->getClass());
        static::assertTrue($definition->hasTag('kernel.event_subscriber'));
    }

    public function test_security_instrumentation_registers_subscriber_and_field_parameters(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'security' => [
                    'enabled' => true,
                    'fields' => [
                        'roles' => ['enabled' => true],
                        'email' => ['enabled' => false],
                    ],
                ],
            ],
        ]], $container);

        static::assertTrue($container->hasDefinition('flow.telemetry.security.user_attribute_resolver'));
        static::assertTrue(
            $container->getDefinition('flow.telemetry.security.span_subscriber')->hasTag('kernel.event_subscriber'),
        );

        static::assertSame('user.id', $container->getParameter('flow.telemetry.security.field.id_attribute'));
        static::assertSame('user.roles', $container->getParameter('flow.telemetry.security.field.roles_attribute'));
        static::assertNull($container->getParameter('flow.telemetry.security.field.email_attribute'));
        static::assertSame('getEmail', $container->getParameter('flow.telemetry.security.field.email_getter'));
    }

    public function test_security_instrumentation_is_not_registered_by_default(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([['resource' => []]], $container);

        static::assertFalse($container->hasDefinition('flow.telemetry.security.span_subscriber'));
    }

    public function test_trace_context_propagation_services_are_registered(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([['resource' => []]], $container);

        static::assertTrue($container->hasDefinition('flow.telemetry.trace_context_provider'));
        static::assertTrue($container->hasAlias(TraceContextProvider::class));
        static::assertTrue($container->hasDefinition('flow.telemetry.trace_context_url_generator'));
        static::assertTrue($container->hasAlias(TraceContextUrlGenerator::class));

        $providerDefinition = $container->getDefinition('flow.telemetry.trace_context_provider');
        static::assertInstanceOf(Reference::class, $providerDefinition->getArgument(2));
        static::assertSame('request_stack', (string) $providerDefinition->getArgument(2));
    }

    public function test_custom_exporter_via_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'custom' => ['service' => ['id' => 'app.my_exporter']],
                    ],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'custom'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $definition = new Definition(VoidExporter::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_exporter', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        $exporter = $container->get('flow.telemetry.exporter.custom');
        static::assertInstanceOf(VoidExporter::class, $exporter);
    }

    public function test_custom_processor_via_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => ['type' => 'service', 'service_id' => 'app.my_span_processor'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $definition = new Definition(VoidSpanProcessor::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_span_processor', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
    }

    public function test_custom_transport_via_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
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
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $definition = new Definition(StreamTransport::class);
                    $definition->setArgument('$destination', 'php://memory');
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_transport', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(StreamTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
    }

    public function test_default_error_handler_is_registered_when_config_omits_error_handlers(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(ErrorLogHandler::class, $container->get('flow.telemetry.error_handler.default'));
    }

    public function test_default_telemetry_is_void(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(Telemetry::class, $container->get(Telemetry::class));
        static::assertInstanceOf(VoidSpanProcessor::class, $container->get('flow.telemetry.tracer_provider.processor'));
        static::assertInstanceOf(
            VoidMetricProcessor::class,
            $container->get('flow.telemetry.meter_provider.processor'),
        );
        static::assertInstanceOf(VoidLogProcessor::class, $container->get('flow.telemetry.logger_provider.processor'));
    }

    public function test_error_log_handler_is_registered_with_configured_args(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => [
                            'type' => 'error_log',
                            'message_type' => 'sapi',
                            'expand_newlines' => true,
                            'message_prefix' => '[custom]',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(ErrorLogHandler::class, $container->get('flow.telemetry.error_handler.default'));
    }

    public function test_minimal_otlp_setup_registers_three_providers_with_one_exporter(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
        static::assertInstanceOf(
            BatchingSpanProcessor::class,
            $container->get('flow.telemetry.tracer_provider.processor'),
        );
        static::assertInstanceOf(
            BatchingMetricProcessor::class,
            $container->get('flow.telemetry.meter_provider.processor'),
        );
        static::assertInstanceOf(
            BatchingLogProcessor::class,
            $container->get('flow.telemetry.logger_provider.processor'),
        );
        static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp'));
    }

    public function test_noop_error_handler_is_registered(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => ['type' => 'error_log'],
                        'silent' => ['type' => 'noop'],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(NullErrorHandler::class, $container->get('flow.telemetry.error_handler.silent'));
    }

    public function test_otlp_exporter_uses_named_error_handler(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'error_handlers' => [
                'default' => ['type' => 'error_log'],
                'silent' => ['type' => 'noop'],
            ],
            'exporters' => [
                'otlp' => [
                    'otlp' => [
                        'error_handler' => 'silent',
                        'transport' => ['type' => 'curl', 'endpoint' => 'http://localhost:4318'],
                    ],
                ],
            ],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.exporter.otlp');
        static::assertInstanceOf(Reference::class, $definition->getArgument(1));
        static::assertSame('flow.telemetry.error_handler.silent', (string) $definition->getArgument(1));
    }

    public function test_messenger_middleware_defaults_to_link_propagation_style(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => [
                    'enabled' => true,
                ],
            ],
        ]], $container);

        static::assertSame(
            MessengerTracePropagation::Link,
            $container->getDefinition('flow.telemetry.messenger.middleware')->getArgument(3),
        );
    }

    public function test_messenger_middleware_receives_continue_propagation_style(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => [
                    'enabled' => true,
                    'context_propagation' => true,
                    'propagation_style' => 'continue',
                ],
            ],
        ]], $container);

        static::assertSame(
            MessengerTracePropagation::Continuation,
            $container->getDefinition('flow.telemetry.messenger.middleware')->getArgument(3),
        );
    }

    public function test_messenger_middleware_receives_link_propagation_style(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => [
                    'enabled' => true,
                    'context_propagation' => true,
                    'propagation_style' => 'link',
                ],
            ],
        ]], $container);

        static::assertSame(
            MessengerTracePropagation::Link,
            $container->getDefinition('flow.telemetry.messenger.middleware')->getArgument(3),
        );
    }

    public function test_messenger_link_to_worker_defaults_to_true_and_can_be_disabled(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $enabled = new ContainerBuilder();
        $enabled->setParameter('kernel.environment', 'test');
        $enabled->setParameter('kernel.project_dir', sys_get_temp_dir());
        $enabled->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => ['messenger' => ['enabled' => true]],
        ]], $enabled);

        $definition = $enabled->getDefinition('flow.telemetry.messenger.middleware');
        static::assertTrue($definition->getArgument(4));
        static::assertInstanceOf(Reference::class, $definition->getArgument(1));

        $disabled = new ContainerBuilder();
        $disabled->setParameter('kernel.environment', 'test');
        $disabled->setParameter('kernel.project_dir', sys_get_temp_dir());
        $disabled->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => ['messenger' => ['enabled' => true, 'link_to_worker' => false]],
        ]], $disabled);

        static::assertFalse($disabled->getDefinition('flow.telemetry.messenger.middleware')->getArgument(4));
    }

    public function test_messenger_context_storage_wired_even_when_propagation_disabled(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => ['messenger' => ['enabled' => true, 'context_propagation' => false]],
        ]], $container);

        static::assertInstanceOf(
            Reference::class,
            $container->getDefinition('flow.telemetry.messenger.middleware')->getArgument(1),
        );
    }

    public function test_messenger_flush_subscriber_is_registered_and_tagged_when_enabled(): void
    {
        if (!interface_exists(MessengerMiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => ['enabled' => true],
            ],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.messenger.flush_subscriber');
        static::assertSame(MessengerFlushSubscriber::class, $definition->getClass());
        static::assertTrue($definition->hasTag('kernel.event_subscriber'));
    }

    public function test_messenger_flush_subscriber_absent_when_messenger_disabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'instrumentation' => [
                'messenger' => ['enabled' => false],
            ],
        ]], $container);

        static::assertFalse($container->hasDefinition('flow.telemetry.messenger.flush_subscriber'));
    }

    public function test_otlp_transport_failover_inline_curl_with_stream_failover(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(CurlTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
        static::assertInstanceOf(
            StreamTransport::class,
            $container->get('flow.telemetry.exporter.otlp.failover.transport'),
        );
    }

    public function test_otlp_transport_failover_inline_grpc_with_curl_failover(): void
    {
        if (!extension_loaded('grpc')) {
            static::markTestSkipped('ext-grpc is required');
        }

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'grpc',
                                    'endpoint' => 'localhost:4317',
                                    'failover' => [
                                        'type' => 'curl',
                                        'endpoint' => 'http://localhost:4318',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(GrpcTransport::class, $container->get('flow.telemetry.exporter.otlp.transport'));
        static::assertInstanceOf(
            CurlTransport::class,
            $container->get('flow.telemetry.exporter.otlp.failover.transport'),
        );
    }

    public function test_processor_referencing_unknown_exporter_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('references unknown exporter "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => ['type' => 'batching', 'exporter' => 'missing'],
                    ],
                ]);
            },
        ]);
    }

    public function test_processor_uses_named_error_handler(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'error_handlers' => [
                'default' => ['type' => 'error_log'],
                'silent' => ['type' => 'noop'],
            ],
            'exporters' => [
                'memory' => ['memory' => null],
            ],
            'logger_provider' => [
                'processor' => [
                    'type' => 'batching',
                    'exporter' => 'memory',
                    'error_handler' => 'silent',
                ],
            ],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.logger_provider.processor');
        static::assertInstanceOf(Reference::class, $definition->getArgument(2));
        static::assertSame('flow.telemetry.error_handler.silent', (string) $definition->getArgument(2));
    }

    public function test_max_batch_age_is_passed_to_batching_processor_definitions(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'exporters' => [
                'memory' => ['memory' => null],
            ],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory', 'max_batch_age' => 15.0],
            ],
            'meter_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory', 'max_batch_age' => 30.0],
            ],
            'logger_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory', 'max_batch_age' => 5.5],
            ],
        ]], $container);

        static::assertSame(15.0, $container->getDefinition('flow.telemetry.tracer_provider.processor')->getArgument(3));
        static::assertSame(30.0, $container->getDefinition('flow.telemetry.meter_provider.processor')->getArgument(3));
        static::assertSame(5.5, $container->getDefinition('flow.telemetry.logger_provider.processor')->getArgument(3));
    }

    public function test_max_batch_age_defaults_to_null_in_batching_processor_definitions(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'exporters' => [
                'memory' => ['memory' => null],
            ],
            'tracer_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory'],
            ],
            'meter_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory'],
            ],
            'logger_provider' => [
                'processor' => ['type' => 'batching', 'exporter' => 'memory'],
            ],
        ]], $container);

        static::assertNull($container->getDefinition('flow.telemetry.tracer_provider.processor')->getArgument(3));
        static::assertNull($container->getDefinition('flow.telemetry.meter_provider.processor')->getArgument(3));
        static::assertNull($container->getDefinition('flow.telemetry.logger_provider.processor')->getArgument(3));
    }

    public function test_provider_uses_named_error_handler(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('kernel.build_dir', sys_get_temp_dir());
        $extension = (new FlowTelemetryBundle())->getContainerExtension();
        assert($extension !== null);
        $extension->load([[
            'resource' => [],
            'error_handlers' => [
                'default' => ['type' => 'error_log'],
                'silent' => ['type' => 'noop'],
            ],
            'logger_provider' => ['error_handler' => 'silent'],
        ]], $container);

        $definition = $container->getDefinition('flow.telemetry.logger_provider');
        static::assertInstanceOf(Reference::class, $definition->getArgument('$errorHandler'));
        static::assertSame('flow.telemetry.error_handler.silent', (string) $definition->getArgument('$errorHandler'));
    }

    public function test_service_error_handler_creates_alias_to_user_service_id(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => ['type' => 'error_log'],
                        'custom' => ['type' => 'service', 'service_id' => 'app.my_handler'],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $definition = new Definition(NullErrorHandler::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.my_handler', $definition);
                });
            },
        ]);

        $container = $this->getContainer();
        static::assertSame($container->get('app.my_handler'), $container->get('flow.telemetry.error_handler.custom'));
    }

    public function test_service_error_handler_missing_service_id_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('error_handler "custom" of type "service" requires a non-empty "service_id"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'custom' => ['type' => 'service'],
                    ],
                ]);
            },
        ]);
    }

    public function test_pipeline_with_severity_middleware_and_batching_sink(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
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
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(
            PipelineLogProcessor::class,
            $container->get('flow.telemetry.logger_provider.processor'),
        );
        static::assertInstanceOf(
            SeverityFilteringLogMiddleware::class,
            $container->get('flow.telemetry.logger_provider.processor.middleware.0'),
        );
        static::assertInstanceOf(
            BatchingLogProcessor::class,
            $container->get('flow.telemetry.logger_provider.processor.sink.processor'),
        );
    }

    public function test_attribute_matching_sampler_with_delegate(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'sampler' => [
                            'type' => 'attribute_matching',
                            'matcher' => ['path' => 'http.route', 'mode' => 'equal', 'value' => '/health'],
                            'delegate' => ['type' => 'trace_id_ratio', 'ratio' => 0.1],
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(
            AttributeMatchingSampler::class,
            $container->get('flow.telemetry.tracer_provider.sampler'),
        );
        static::assertInstanceOf(
            AttributeFilter::class,
            $container->get('flow.telemetry.tracer_provider.sampler.filter'),
        );
        static::assertInstanceOf(
            TraceIdRatioBasedSampler::class,
            $container->get('flow.telemetry.tracer_provider.sampler.delegate'),
        );
    }

    public function test_stream_handler_is_registered_with_destination(): void
    {
        $destination = sys_get_temp_dir() . '/flow-telemetry-test-' . uniqid() . '.log';

        try {
            $this->bootKernel([
                'config' => static function (TestKernel $kernel) use ($destination): void {
                    $kernel->addTestExtensionConfig('flow_telemetry', [
                        'resource' => [],
                        'error_handlers' => [
                            'default' => ['type' => 'error_log'],
                            'to_file' => [
                                'type' => 'stream',
                                'destination' => $destination,
                                'create_directories' => false,
                            ],
                        ],
                    ]);
                },
            ]);

            $container = $this->getContainer();
            static::assertInstanceOf(StreamHandler::class, $container->get('flow.telemetry.error_handler.to_file'));
        } finally {
            if (is_file($destination)) {
                unlink($destination);
            }
        }
    }

    public function test_stream_handler_missing_destination_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('error_handler "to_file" of type "stream" requires a non-empty "destination"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'to_file' => ['type' => 'stream'],
                    ],
                ]);
            },
        ]);
    }

    public function test_stream_transport_is_built_inline_for_file_path(): void
    {
        $path = sys_get_temp_dir() . '/flow-otlp-bundle-' . bin2hex(random_bytes(4)) . '.jsonl';

        try {
            $this->bootKernel([
                'config' => static function (TestKernel $kernel) use ($path): void {
                    $kernel->addTestExtensionConfig('flow_telemetry', [
                        'resource' => [],
                        'exporters' => [
                            'otlp_stream' => [
                                'otlp' => [
                                    'transport' => [
                                        'type' => 'stream',
                                        'endpoint' => $path,
                                        'file_permissions' => 0o640,
                                        'create_directories' => true,
                                    ],
                                ],
                            ],
                        ],
                    ]);
                },
            ]);

            $container = $this->getContainer();
            static::assertInstanceOf(
                StreamTransport::class,
                $container->get('flow.telemetry.exporter.otlp_stream.transport'),
            );
            static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_stream'));
        } finally {
            if (is_file($path)) {
                unlink($path);
            }
        }
    }

    #[TestWith(['php://stdout'])]
    #[TestWith(['php://stderr'])]
    public function test_stream_transport_is_built_inline_for_php_uri(string $endpoint): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($endpoint): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
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
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(
            StreamTransport::class,
            $container->get('flow.telemetry.exporter.otlp_stream.transport'),
        );
        static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_stream'));
    }

    public function test_syslog_handler_is_registered_with_facility_and_severity(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => ['type' => 'error_log'],
                        'sys' => [
                            'type' => 'syslog',
                            'ident' => 'flow-test',
                            'facility' => 'local3',
                            'severity' => 'warning',
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(SyslogHandler::class, $container->get('flow.telemetry.error_handler.sys'));
    }

    public function test_two_separate_otlp_backends(): void
    {
        if (!extension_loaded('grpc')) {
            static::markTestSkipped(
                'grpc PHP extension is required to instantiate GrpcTransport during container compilation',
            );
        }

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'otlp_traces' => [
                            'otlp' => [
                                'transport' => [
                                    'type' => 'grpc',
                                    'endpoint' => 'http://traces:4317',
                                    'insecure' => true,
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
        static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_traces'));
        static::assertInstanceOf(OTLPExporter::class, $container->get('flow.telemetry.exporter.otlp_metrics'));

        static::assertInstanceOf(
            GrpcTransport::class,
            $container->get('flow.telemetry.exporter.otlp_traces.transport'),
        );
        static::assertInstanceOf(
            CurlTransport::class,
            $container->get('flow.telemetry.exporter.otlp_metrics.transport'),
        );
    }

    public function test_udp_syslog_handler_is_registered_with_host_and_port(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'error_handlers' => [
                        'default' => ['type' => 'error_log'],
                        'remote' => [
                            'type' => 'udp_syslog',
                            'host' => '192.0.2.1',
                            'port' => 1514,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();
        static::assertInstanceOf(UdpSyslogHandler::class, $container->get('flow.telemetry.error_handler.remote'));
    }

    public function test_unknown_error_handler_reference_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unknown error_handler "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => [
                        'memory' => ['memory' => null],
                    ],
                    'logger_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'exporter' => 'memory',
                            'error_handler' => 'missing',
                        ],
                    ],
                ]);
            },
        ]);
    }
}
