<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection;

use Flow\Bridge\Psr3\Telemetry\{LogRecordConverter, TelemetryLogger};
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Resource\Detector\SymfonyDeploymentDetector;
use Flow\Bridge\Telemetry\OTLP\Exporter\{OTLPLogExporter, OTLPMetricExporter, OTLPSpanExporter};
use Flow\Bridge\Telemetry\OTLP\Serializer\{JsonSerializer, ProtobufSerializer};
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, CurlTransportOptions, GrpcTransport, HttpTransport};
use Flow\Telemetry\{Attributes, Logger\Logger, Meter\Meter, Tracer\Tracer};
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{LoggerProvider, Severity};
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor,
    CompositeLogProcessor,
    PassThroughLogProcessor,
    SeverityFilteringLogProcessor};
use Flow\Telemetry\Meter\{AggregationTemporality, MeterProvider};
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, CompositeMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Propagation\{CompositePropagator, W3CBaggage, W3CTraceContext};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleMetricExporter, ConsoleSpanExporter};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter,
    MemoryLogProcessor,
    MemoryMetricExporter,
    MemoryMetricProcessor,
    MemorySpanExporter,
    MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter,
    VoidLogProcessor,
    VoidMetricExporter,
    VoidMetricProcessor,
    VoidSpanExporter,
    VoidSpanProcessor};
use Flow\Telemetry\Resource\Detector\{CachingDetector,
    ChainDetector,
    ComposerDetector,
    EnvironmentDetector,
    HostDetector,
    ManualDetector,
    OsDetector,
    ProcessDetector};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\Processor\{BatchingSpanProcessor, CompositeSpanProcessor, PassThroughSpanProcessor};
use Flow\Telemetry\Tracer\Sampler\{AlwaysOffSampler, AlwaysOnSampler, ParentBasedSampler, TraceIdRatioBasedSampler};
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Twig\Extension\AbstractExtension;

final class FlowTelemetryExtension extends Extension
{
    private const string MESSENGER_MIDDLEWARE_INTERFACE = 'Symfony\\Component\\Messenger\\Middleware\\MiddlewareInterface';

    /** @var array<string, bool> */
    private array $configsEnabled = [];

    /**
     * @param array<array-key, mixed> $configs
     */
    public function load(array $configs, ContainerBuilder $container) : void
    {
        $loader = new PhpFileLoader($container, new FileLocator(__DIR__ . '/../Resources/config'));
        $configuration = new Configuration();
        /** @var array{resource: array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>}, clock_service_id?: null|string, main_logger?: null|string, context_storage?: array{type?: string, service_id?: null|string}, propagator?: array{type?: string, service_id?: null|string}, tracer_provider?: array<string, mixed>, meter_provider?: array<string, mixed>, logger_provider?: array<string, mixed>, instrumentation?: array{http_kernel?: array{enabled?: bool, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}}, tracers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>, meters?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>, loggers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>} $config */
        $config = $this->processConfiguration($configuration, $configs);

        $container->setParameter('flow.telemetry.main_logger', $config['main_logger'] ?? null);

        $tracers = ($config['tracers'] ?? []) + ['default' => []];
        $meters = ($config['meters'] ?? []) + ['default' => []];
        $loggers = ($config['loggers'] ?? []) + ['default' => []];

        $this->registerGlobalServices($config, $container);
        $this->registerPropagator($config['propagator'] ?? [], $container);
        $this->registerResource($config['resource'], $container);
        $this->registerTelemetry($config, $container);
        $this->registerInstrumentation($config['instrumentation'] ?? [], $container, $loader);
        $this->registerTracers($tracers, $container);
        $this->registerMeters($meters, $container);
        $this->registerLoggers($loggers, $container);
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildInnerLogProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $container->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($processorServiceId, new Definition(VoidLogProcessor::class));

                break;

            case 'memory':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(PassThroughLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown inner log processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildLogExporter(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $exporterServiceId = $serviceIdPrefix . '.exporter';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when exporter type is "service"');
                }
                $container->setAlias($exporterServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($exporterServiceId, new Definition(VoidLogExporter::class));

                break;

            case 'memory':
                $container->setDefinition($exporterServiceId, new Definition(MemoryLogExporter::class));

                break;

            case 'console':
                $container->setDefinition($exporterServiceId, new Definition(ConsoleLogExporter::class));

                break;

            case 'otlp':
                $container->setParameter('flow.telemetry.otlp_configured', true);
                $transportServiceId = $this->buildOTLPTransport(
                    $config['otlp']['transport'] ?? [],
                    $exporterServiceId,
                    $container
                );
                $definition = new Definition(OTLPLogExporter::class);
                $definition->setArgument(0, new Reference($transportServiceId));
                $container->setDefinition($exporterServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown log exporter type: %s', (string) $type));
        }

        return $exporterServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildLoggerProvider(array $config, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.logger_provider';

        $processorServiceId = $this->buildLogProcessor($config['processor'] ?? [], $providerServiceId, $container);

        $definition = new Definition(LoggerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildLogProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $container->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($processorServiceId, new Definition(VoidLogProcessor::class));

                break;

            case 'memory':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildLogExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(PassThroughLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildLogProcessor(
                        $processorConfig,
                        $processorServiceId . '.' . $idx,
                        $container
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeLogProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'severity_filtering':
                $innerProcessorConfig = $config['inner_processor'] ?? [];
                $innerProcessorServiceId = $this->buildInnerLogProcessor(
                    $innerProcessorConfig,
                    $processorServiceId . '.inner',
                    $container
                );
                $minimumSeverity = $this->mapSeverity($config['minimum_severity'] ?? 'info');
                $definition = new Definition(SeverityFilteringLogProcessor::class);
                $definition->setArgument(0, new Reference($innerProcessorServiceId));
                $definition->setArgument(1, $minimumSeverity);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown log processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildMeterProvider(array $config, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.meter_provider';

        $processorServiceId = $this->buildMetricProcessor($config['processor'] ?? [], $providerServiceId, $container);

        $temporality = ($config['temporality'] ?? 'cumulative') === 'delta'
            ? AggregationTemporality::DELTA
            : AggregationTemporality::CUMULATIVE;

        $definition = new Definition(MeterProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, $temporality);
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildMetricExporter(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $exporterServiceId = $serviceIdPrefix . '.exporter';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when exporter type is "service"');
                }
                $container->setAlias($exporterServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($exporterServiceId, new Definition(VoidMetricExporter::class));

                break;

            case 'memory':
                $container->setDefinition($exporterServiceId, new Definition(MemoryMetricExporter::class));

                break;

            case 'console':
                $container->setDefinition($exporterServiceId, new Definition(ConsoleMetricExporter::class));

                break;

            case 'otlp':
                $container->setParameter('flow.telemetry.otlp_configured', true);
                $transportServiceId = $this->buildOTLPTransport(
                    $config['otlp']['transport'] ?? [],
                    $exporterServiceId,
                    $container
                );
                $definition = new Definition(OTLPMetricExporter::class);
                $definition->setArgument(0, new Reference($transportServiceId));
                $container->setDefinition($exporterServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown metric exporter type: %s', (string) $type));
        }

        return $exporterServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildMetricProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $container->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($processorServiceId, new Definition(VoidMetricProcessor::class));

                break;

            case 'memory':
                $exporterServiceId = $this->buildMetricExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(MemoryMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildMetricExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(BatchingMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildMetricExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(PassThroughMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildMetricProcessor(
                        $processorConfig,
                        $processorServiceId . '.' . $idx,
                        $container
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeMetricProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown metric processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array{type?: string, service_id?: string} $config
     */
    private function buildOTLPSerializer(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $serializerServiceId = $serviceIdPrefix . '.serializer';
        $type = $config['type'] ?? 'json';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when serializer type is "service"');
                }
                $container->setAlias($serializerServiceId, $customServiceId);

                break;

            case 'json':
                $definition = new Definition(JsonSerializer::class);
                $container->setDefinition($serializerServiceId, $definition);

                break;

            case 'protobuf':
                $definition = new Definition(ProtobufSerializer::class);
                $container->setDefinition($serializerServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown OTLP serializer type: %s', (string) $type));
        }

        return $serializerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildOTLPTransport(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $transportServiceId = $serviceIdPrefix . '.transport';
        $type = $config['type'];

        if ($type === 'service') {
            $customServiceId = $config['service_id'] ?? null;

            if ($customServiceId === null) {
                throw new RuntimeException('service_id is required when transport type is "service"');
            }
            $container->setAlias($transportServiceId, $customServiceId);

            return $transportServiceId;
        }

        $endpoint = $config['endpoint'];
        $timeout = $config['timeout'];
        $headers = $config['headers'];

        $serializerServiceId = $this->buildOTLPSerializer($config['serializer'], $transportServiceId, $container);

        switch ($type) {
            case 'curl':
                $optionsServiceId = $transportServiceId . '.options';
                $optionsDefinition = new Definition(CurlTransportOptions::class);
                $optionsDefinition->addMethodCall('withTimeout', [$timeout]);
                $optionsDefinition->addMethodCall('withConnectTimeout', [$config['connect_timeout']]);

                foreach ($headers as $headerName => $headerValue) {
                    $optionsDefinition->addMethodCall('withHeader', [(string) $headerName, (string) $headerValue]);
                }

                if ($config['compression']) {
                    $optionsDefinition->addMethodCall('withCompression', [true]);
                }

                $optionsDefinition->addMethodCall('withFollowRedirects', [
                    $config['follow_redirects'],
                    $config['max_redirects'],
                ]);

                if ($config['proxy'] !== null) {
                    $optionsDefinition->addMethodCall('withProxy', [$config['proxy']]);
                }

                $optionsDefinition->addMethodCall('withSslVerification', [
                    $config['ssl_verify_peer'],
                    $config['ssl_verify_host'],
                ]);

                if ($config['ssl_cert_path'] !== null) {
                    $optionsDefinition->addMethodCall('withSslCertificate', [
                        $config['ssl_cert_path'],
                        $config['ssl_key_path'],
                    ]);
                }

                if ($config['ca_info_path'] !== null) {
                    $optionsDefinition->addMethodCall('withCaInfo', [$config['ca_info_path']]);
                }

                $container->setDefinition($optionsServiceId, $optionsDefinition);

                $definition = new Definition(CurlTransport::class);
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Reference($serializerServiceId));
                $definition->setArgument(2, new Reference($optionsServiceId));
                $container->setDefinition($transportServiceId, $definition);

                break;

            case 'http':
                $httpClientServiceId = $config['http_client_service_id'] ?? null;
                $requestFactoryServiceId = $config['request_factory_service_id'] ?? null;
                $streamFactoryServiceId = $config['stream_factory_service_id'] ?? null;

                if ($httpClientServiceId === null) {
                    throw new RuntimeException('http_client_service_id is required when transport type is "http"');
                }

                if ($requestFactoryServiceId === null) {
                    throw new RuntimeException('request_factory_service_id is required when transport type is "http"');
                }

                if ($streamFactoryServiceId === null) {
                    throw new RuntimeException('stream_factory_service_id is required when transport type is "http"');
                }

                $definition = new Definition(HttpTransport::class);
                $definition->setArgument('$httpClient', new Reference($httpClientServiceId));
                $definition->setArgument('$requestFactory', new Reference($requestFactoryServiceId));
                $definition->setArgument('$streamFactory', new Reference($streamFactoryServiceId));
                $definition->setArgument('$endpoint', $endpoint);
                $definition->setArgument('$serializer', new Reference($serializerServiceId));
                $definition->setArgument('$headers', $headers);
                $container->setDefinition($transportServiceId, $definition);

                break;

            case 'grpc':
                $insecure = $config['insecure'];
                $definition = new Definition(GrpcTransport::class);
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Reference($serializerServiceId));
                $definition->setArgument(2, $headers);
                $definition->setArgument(3, $insecure);
                $container->setDefinition($transportServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown OTLP transport type: %s', (string) $type));
        }

        return $transportServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildSampler(array $config, ContainerBuilder $container) : string
    {
        $samplerServiceId = 'flow.telemetry.tracer_provider.sampler';
        $type = $config['type'] ?? 'always_on';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when sampler type is "service"');
                }
                $container->setAlias($samplerServiceId, $customServiceId);

                break;

            case 'always_on':
                $container->setDefinition($samplerServiceId, new Definition(AlwaysOnSampler::class));

                break;

            case 'always_off':
                $container->setDefinition($samplerServiceId, new Definition(AlwaysOffSampler::class));

                break;

            case 'trace_id_ratio':
                $definition = new Definition(TraceIdRatioBasedSampler::class);
                $definition->setArgument(0, $config['ratio'] ?? 1.0);
                $container->setDefinition($samplerServiceId, $definition);

                break;

            case 'parent_based':
                $rootSamplerServiceId = $samplerServiceId . '.root';
                $rootSamplerDefinition = new Definition(AlwaysOnSampler::class);
                $container->setDefinition($rootSamplerServiceId, $rootSamplerDefinition);

                $definition = new Definition(ParentBasedSampler::class);
                $definition->setArgument(0, new Reference($rootSamplerServiceId));
                $container->setDefinition($samplerServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown sampler type: %s', (string) $type));
        }

        return $samplerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildSpanExporter(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $exporterServiceId = $serviceIdPrefix . '.exporter';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when exporter type is "service"');
                }
                $container->setAlias($exporterServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($exporterServiceId, new Definition(VoidSpanExporter::class));

                break;

            case 'memory':
                $container->setDefinition($exporterServiceId, new Definition(MemorySpanExporter::class));

                break;

            case 'console':
                $container->setDefinition($exporterServiceId, new Definition(ConsoleSpanExporter::class));

                break;

            case 'otlp':
                $container->setParameter('flow.telemetry.otlp_configured', true);
                $transportServiceId = $this->buildOTLPTransport(
                    $config['otlp']['transport'] ?? [],
                    $exporterServiceId,
                    $container
                );
                $definition = new Definition(OTLPSpanExporter::class);
                $definition->setArgument(0, new Reference($transportServiceId));
                $container->setDefinition($exporterServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown span exporter type: %s', (string) $type));
        }

        return $exporterServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildSpanProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container) : string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $container->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $container->setDefinition($processorServiceId, new Definition(VoidSpanProcessor::class));

                break;

            case 'memory':
                $exporterServiceId = $this->buildSpanExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(MemorySpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildSpanExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(BatchingSpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildSpanExporter(
                    $config['exporter'] ?? [],
                    $processorServiceId,
                    $container
                );
                $definition = new Definition(PassThroughSpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildSpanProcessor(
                        $processorConfig,
                        $processorServiceId . '.' . $idx,
                        $container
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeSpanProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown span processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildTracerProvider(array $config, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.tracer_provider';

        $processorServiceId = $this->buildSpanProcessor($config['processor'] ?? [], $providerServiceId, $container);
        $samplerServiceId = $this->buildSampler($config['sampler'] ?? [], $container);

        $definition = new Definition(TracerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $definition->setArgument(3, new Reference($samplerServiceId));
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    private function mapSeverity(string $severity) : Severity
    {
        return match ($severity) {
            'trace' => Severity::TRACE,
            'debug' => Severity::DEBUG,
            'info' => Severity::INFO,
            'warn' => Severity::WARN,
            'error' => Severity::ERROR,
            'fatal' => Severity::FATAL,
            default => throw new RuntimeException(\sprintf('Unknown severity level: %s', $severity)),
        };
    }

    /**
     * @param array<string, mixed> $config
     */
    private function readConfigEnabled(string $path, ContainerBuilder $container, array $config) : bool
    {
        return $this->configsEnabled[$path] ??= parent::isConfigEnabled($container, $config);
    }

    /**
     * @param array<string, mixed> $config
     */
    private function registerGlobalServices(array $config, ContainerBuilder $container) : void
    {
        $clockServiceId = $config['clock_service_id'] ?? null;

        if ($clockServiceId !== null) {
            $container->setAlias('flow.telemetry.clock', $clockServiceId);
        } elseif ($container->has(ClockInterface::class)) {
            $container->setAlias('flow.telemetry.clock', ClockInterface::class);
        } else {
            $container->setDefinition('flow.telemetry.clock', new Definition(SystemClock::class));
        }

        $contextStorageConfig = $config['context_storage'];
        $contextStorageType = $contextStorageConfig['type'];

        if ($contextStorageType === 'service') {
            $customServiceId = $contextStorageConfig['service_id'] ?? null;

            if ($customServiceId === null) {
                throw new RuntimeException('service_id is required when context_storage type is "service"');
            }
            $container->setAlias('flow.telemetry.context_storage', $customServiceId);
        } else {
            $container->setDefinition('flow.telemetry.context_storage', new Definition(MemoryContextStorage::class));
        }

        $container->setDefinition(
            'flow.telemetry.psr3.log_record_converter',
            new Definition(LogRecordConverter::class)
        );
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}} $config
     */
    private function registerInstrumentation(array $config, ContainerBuilder $container, PhpFileLoader $loader) : void
    {
        $httpKernelConfig = $config['http_kernel'] ?? [];

        if ($this->readConfigEnabled('instrumentation.http_kernel', $container, $httpKernelConfig)) {
            $container->setParameter('flow.telemetry.http_kernel.exclude_paths', $httpKernelConfig['exclude_paths'] ?? []);
            $container->setParameter('flow.telemetry.http_kernel.context_propagation', $httpKernelConfig['context_propagation'] ?? true);
            $loader->load('instrumentation/http_kernel.php');
        }

        $consoleConfig = $config['console'] ?? [];

        if ($this->readConfigEnabled('instrumentation.console', $container, $consoleConfig)) {
            $container->setParameter('flow.telemetry.console.exclude_commands', $consoleConfig['exclude_commands'] ?? []);
            $loader->load('instrumentation/console.php');
        }

        $messengerConfig = $config['messenger'] ?? [];

        if ($this->readConfigEnabled('instrumentation.messenger', $container, $messengerConfig)) {
            if (!\interface_exists(self::MESSENGER_MIDDLEWARE_INTERFACE)) {
                throw new RuntimeException('Messenger instrumentation requires symfony/messenger package. Install it via composer: composer require symfony/messenger');
            }

            $loader->load('instrumentation/messenger.php');

            if ($messengerConfig['context_propagation'] ?? true) {
                $definition = $container->getDefinition('flow.telemetry.messenger.middleware');
                $definition->setArgument(1, new Reference('flow.telemetry.context_storage'));
                $definition->setArgument(2, new Reference('flow.telemetry.propagator'));
            }
        }

        $twigConfig = $config['twig'] ?? [];

        if ($this->readConfigEnabled('instrumentation.twig', $container, $twigConfig)) {
            if (!\class_exists(AbstractExtension::class)) {
                throw new RuntimeException('Twig instrumentation requires twig/twig package. Install it via composer: composer require twig/twig');
            }

            $container->setParameter('flow.telemetry.twig.trace_templates', $twigConfig['trace_templates'] ?? true);
            $container->setParameter('flow.telemetry.twig.trace_blocks', $twigConfig['trace_blocks'] ?? false);
            $container->setParameter('flow.telemetry.twig.trace_macros', $twigConfig['trace_macros'] ?? false);
            $container->setParameter('flow.telemetry.twig.exclude_templates', $twigConfig['exclude_templates'] ?? []);
            $loader->load('instrumentation/twig.php');
        }

        $this->registerParameterOnlyInstrumentation($config, $container);
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}> $config
     */
    private function registerLoggers(array $config, ContainerBuilder $container) : void
    {
        foreach ($config as $name => $loggerConfig) {
            $definition = new Definition(Logger::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'logger']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $loggerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $loggerConfig['schema_url'] ?? null);

            $attributes = $loggerConfig['attributes'] ?? [];

            if (\count($attributes) > 0) {
                $attributesDefinition = new Definition(Attributes::class);
                $attributesDefinition->setFactory([Attributes::class, 'create']);
                $attributesDefinition->setArgument(0, $attributes);
                $definition->setArgument(3, $attributesDefinition);
            } else {
                $definition->setArgument(3, null);
            }

            $definition->setPublic(true);
            $loggerServiceId = 'flow.telemetry.' . $name . '.logger';
            $container->setDefinition($loggerServiceId, $definition);

            $psr3Definition = new Definition(TelemetryLogger::class);
            $psr3Definition->setArgument(0, new Reference($loggerServiceId));
            $psr3Definition->setArgument(1, new Reference('flow.telemetry.psr3.log_record_converter'));
            $psr3Definition->setPublic(true);
            $container->setDefinition($loggerServiceId . '.psr3', $psr3Definition);
        }
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}> $config
     */
    private function registerMeters(array $config, ContainerBuilder $container) : void
    {
        foreach ($config as $name => $meterConfig) {
            $definition = new Definition(Meter::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'meter']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $meterConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $meterConfig['schema_url'] ?? null);

            $attributes = $meterConfig['attributes'] ?? [];

            if (\count($attributes) > 0) {
                $attributesDefinition = new Definition(Attributes::class);
                $attributesDefinition->setFactory([Attributes::class, 'create']);
                $attributesDefinition->setArgument(0, $attributes);
                $definition->setArgument(3, $attributesDefinition);
            } else {
                $definition->setArgument(3, null);
            }

            $definition->setPublic(true);
            $container->setDefinition('flow.telemetry.' . $name . '.meter', $definition);
        }
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}} $config
     */
    private function registerParameterOnlyInstrumentation(array $config, ContainerBuilder $container) : void
    {
        $httpClientConfig = $config['http_client'] ?? [];
        $container->setParameter(
            'flow.telemetry.http_client.enabled',
            $httpClientConfig['enabled'] ?? false
        );
        $container->setParameter(
            'flow.telemetry.http_client.exclude_clients',
            $httpClientConfig['exclude_clients'] ?? []
        );

        $psr18ClientConfig = $config['psr18_client'] ?? [];
        $container->setParameter(
            'flow.telemetry.psr18_client.enabled',
            $psr18ClientConfig['enabled'] ?? false
        );
        $container->setParameter(
            'flow.telemetry.psr18_client.exclude_clients',
            $psr18ClientConfig['exclude_clients'] ?? []
        );

        $dbalConfig = $config['dbal'] ?? [];
        $container->setParameter(
            'flow.telemetry.dbal.enabled',
            $dbalConfig['enabled'] ?? false
        );
        $container->setParameter(
            'flow.telemetry.dbal.log_sql',
            $dbalConfig['log_sql'] ?? true
        );
        $container->setParameter(
            'flow.telemetry.dbal.max_sql_length',
            $dbalConfig['max_sql_length'] ?? 1000
        );
        $container->setParameter(
            'flow.telemetry.dbal.exclude_connections',
            $dbalConfig['exclude_connections'] ?? []
        );

        $cacheConfig = $config['cache'] ?? [];
        $container->setParameter(
            'flow.telemetry.cache.enabled',
            $cacheConfig['enabled'] ?? false
        );
        $container->setParameter(
            'flow.telemetry.cache.exclude_pools',
            $cacheConfig['exclude_pools'] ?? []
        );
    }

    /**
     * @param array{type?: string, service_id?: null|string} $config
     */
    private function registerPropagator(array $config, ContainerBuilder $container) : void
    {
        $type = $config['type'] ?? 'w3c';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when propagator type is "service"');
                }
                $container->setAlias('flow.telemetry.propagator', $customServiceId);

                break;

            case 'w3c':
                $container->setDefinition('flow.telemetry.propagator.tracecontext', new Definition(W3CTraceContext::class));
                $container->setDefinition('flow.telemetry.propagator.baggage', new Definition(W3CBaggage::class));

                $compositeDefinition = new Definition(CompositePropagator::class);
                $compositeDefinition->setArgument(0, [
                    new Reference('flow.telemetry.propagator.tracecontext'),
                    new Reference('flow.telemetry.propagator.baggage'),
                ]);
                $container->setDefinition('flow.telemetry.propagator', $compositeDefinition);

                break;

            case 'tracecontext':
                $container->setDefinition('flow.telemetry.propagator', new Definition(W3CTraceContext::class));

                break;

            case 'baggage':
                $container->setDefinition('flow.telemetry.propagator', new Definition(W3CBaggage::class));

                break;

            default:
                throw new RuntimeException(\sprintf('Unknown propagator type: %s', (string) $type));
        }
    }

    /**
     * @param array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>} $resourceConfig
     */
    private function registerResource(array $resourceConfig, ContainerBuilder $container) : void
    {
        $detectorsConfig = $resourceConfig['detectors'] ?? [];
        $detectorsEnabled = $detectorsConfig['enabled'] ?? true;

        if (!$detectorsEnabled) {
            $customAttributes = $resourceConfig['custom'] ?? [];
            $definition = new Definition(Resource::class);
            $definition->setFactory([Resource::class, 'create']);
            $definition->setArgument(0, $customAttributes);
            $container->setDefinition('flow.telemetry.resource', $definition);

            return;
        }

        $staticConfig = $detectorsConfig['static'] ?? [];
        $dynamicConfig = $detectorsConfig['dynamic'] ?? [];
        $customAttributes = $resourceConfig['custom'] ?? [];

        $staticDetectorRefs = [];

        if ($staticConfig['os']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.os',
                new Definition(OsDetector::class)
            );
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.os');
        }

        if ($staticConfig['host']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.host',
                new Definition(HostDetector::class)
            );
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.host');
        }

        if ($staticConfig['service']['enabled'] ?? true) {
            $composerDefinition = new Definition(ComposerDetector::class);
            $composerDefinition->setArgument(0, '%kernel.project_dir%/composer.json');
            $container->setDefinition('flow.telemetry.resource.detector.service', $composerDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.service');
        }

        if ($staticConfig['deployment']['enabled'] ?? true) {
            $deploymentDefinition = new Definition(SymfonyDeploymentDetector::class);
            $deploymentDefinition->setArgument(0, '%kernel.environment%');
            $container->setDefinition('flow.telemetry.resource.detector.deployment', $deploymentDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.deployment');
        }

        if (\count($customAttributes) > 0) {
            $manualDefinition = new Definition(ManualDetector::class);
            $manualDefinition->setArgument(0, $customAttributes);
            $container->setDefinition('flow.telemetry.resource.detector.custom', $manualDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.custom');
        }

        if ($staticConfig['environment']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.environment',
                new Definition(EnvironmentDetector::class)
            );
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.environment');
        }

        $staticChainDefinition = new Definition(ChainDetector::class);
        $staticChainDefinition->setArguments($staticDetectorRefs);
        $container->setDefinition('flow.telemetry.resource.detector.static.chain', $staticChainDefinition);

        $cacheConfig = $staticConfig['cache'] ?? [];
        $cacheEnabled = $cacheConfig['enabled'] ?? true;

        if ($cacheEnabled) {
            $cachingDefinition = new Definition(CachingDetector::class);
            $cachingDefinition->setArgument(0, new Reference('flow.telemetry.resource.detector.static.chain'));
            $cachingDefinition->setArgument(1, $cacheConfig['path'] ?? null);
            $container->setDefinition('flow.telemetry.resource.detector.static', $cachingDefinition);
        } else {
            $container->setAlias('flow.telemetry.resource.detector.static', 'flow.telemetry.resource.detector.static.chain');
        }

        $dynamicDetectorRefs = [];

        if ($dynamicConfig['process']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.process',
                new Definition(ProcessDetector::class)
            );
            $dynamicDetectorRefs[] = new Reference('flow.telemetry.resource.detector.process');
        }

        if (\count($dynamicDetectorRefs) > 0) {
            $dynamicChainDefinition = new Definition(ChainDetector::class);
            $dynamicChainDefinition->setArguments($dynamicDetectorRefs);
            $container->setDefinition('flow.telemetry.resource.detector.dynamic', $dynamicChainDefinition);

            $finalChainDefinition = new Definition(ChainDetector::class);
            $finalChainDefinition->setArguments([
                new Reference('flow.telemetry.resource.detector.static'),
                new Reference('flow.telemetry.resource.detector.dynamic'),
            ]);
            $container->setDefinition('flow.telemetry.resource.detector', $finalChainDefinition);
        } else {
            $container->setAlias('flow.telemetry.resource.detector', 'flow.telemetry.resource.detector.static');
        }

        $resourceDefinition = new Definition(Resource::class);
        $resourceDefinition->setFactory([new Reference('flow.telemetry.resource.detector'), 'detect']);
        $container->setDefinition('flow.telemetry.resource', $resourceDefinition);
    }

    /**
     * @param array<string, mixed> $config
     */
    private function registerTelemetry(array $config, ContainerBuilder $container) : void
    {
        $tracerProviderServiceId = $this->buildTracerProvider($config['tracer_provider'] ?? [], $container);
        $meterProviderServiceId = $this->buildMeterProvider($config['meter_provider'] ?? [], $container);
        $loggerProviderServiceId = $this->buildLoggerProvider($config['logger_provider'] ?? [], $container);

        $telemetryServiceId = 'flow.telemetry';
        $definition = new Definition(Telemetry::class);
        $definition->setArgument(0, new Reference('flow.telemetry.resource'));
        $definition->setArgument(1, new Reference($tracerProviderServiceId));
        $definition->setArgument(2, new Reference($meterProviderServiceId));
        $definition->setArgument(3, new Reference($loggerProviderServiceId));
        $definition->setPublic(true);
        $container->setDefinition($telemetryServiceId, $definition);

        $container->setAlias(Telemetry::class, $telemetryServiceId)->setPublic(true);
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}> $config
     */
    private function registerTracers(array $config, ContainerBuilder $container) : void
    {
        foreach ($config as $name => $tracerConfig) {
            $definition = new Definition(Tracer::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'tracer']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $tracerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $tracerConfig['schema_url'] ?? null);

            $attributes = $tracerConfig['attributes'] ?? [];

            if (\count($attributes) > 0) {
                $attributesDefinition = new Definition(Attributes::class);
                $attributesDefinition->setFactory([Attributes::class, 'create']);
                $attributesDefinition->setArgument(0, $attributes);
                $definition->setArgument(3, $attributesDefinition);
            } else {
                $definition->setArgument(3, null);
            }

            $definition->setPublic(true);
            $container->setDefinition('flow.telemetry.' . $name . '.tracer', $definition);
        }
    }
}
