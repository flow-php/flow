<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection;

use Flow\Bridge\Psr3\Telemetry\LogRecordConverter;
use Flow\Bridge\Psr3\Telemetry\TelemetryLogger;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Resource\Detector\SymfonyDeploymentDetector;
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\ErrorHandler\CompositeErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\ErrorHandler\StreamHandler;
use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogHandler;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;
use Flow\Telemetry\ErrorHandler\UdpSyslogHandler;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Processor\CompositeLogProcessor;
use Flow\Telemetry\Logger\Processor\PassThroughLogProcessor;
use Flow\Telemetry\Logger\Processor\SeverityFilteringLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Meter\Processor\CompositeMetricProcessor;
use Flow\Telemetry\Meter\Processor\PassThroughMetricProcessor;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\ConsoleExporter;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Resource\Detector\CachingDetector;
use Flow\Telemetry\Resource\Detector\ChainDetector;
use Flow\Telemetry\Resource\Detector\ComposerDetector;
use Flow\Telemetry\Resource\Detector\EnvironmentDetector;
use Flow\Telemetry\Resource\Detector\HostDetector;
use Flow\Telemetry\Resource\Detector\ManualDetector;
use Flow\Telemetry\Resource\Detector\OsDetector;
use Flow\Telemetry\Resource\Detector\ProcessDetector;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\Processor\CompositeSpanProcessor;
use Flow\Telemetry\Tracer\Processor\PassThroughSpanProcessor;
use Flow\Telemetry\Tracer\Sampler\AlwaysOffSampler;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\ParentBasedSampler;
use Flow\Telemetry\Tracer\Sampler\TraceIdRatioBasedSampler;
use Flow\Telemetry\Tracer\Tracer;
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Symfony\Component\DependencyInjection\Reference;
use Twig\Extension\AbstractExtension;

use function array_key_exists;
use function class_exists;
use function count;
use function interface_exists;
use function is_array;
use function is_string;
use function sprintf;
use function ucfirst;

use const LOG_PID;

final class FlowTelemetryExtension extends Extension
{
    private const string MESSENGER_MIDDLEWARE_INTERFACE = 'Symfony\\Component\\Messenger\\Middleware\\MiddlewareInterface';

    /** @var array<string, bool> */
    private array $configsEnabled = [];

    /**
     * @param array<array-key, mixed> $configs
     */
    public function load(array $configs, ContainerBuilder $container): void
    {
        $loader = new PhpFileLoader($container, new FileLocator(__DIR__ . '/../Resources/config'));
        $configuration = new Configuration();
        /** @var array{resource: array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>}, clock_service_id?: null|string, framework_logger?: null|string, context_storage?: array{type?: string, service_id?: null|string}, propagator?: array{type?: string, service_id?: null|string}, exporters?: array<string, array<string, mixed>>, tracer_provider?: array<string, mixed>, meter_provider?: array<string, mixed>, logger_provider?: array<string, mixed>, instrumentation?: array{http_kernel?: array{enabled?: bool, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}}, tracers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>, meters?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>, loggers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}>} $config */
        $config = $this->processConfiguration($configuration, $configs);

        $container->setParameter('flow.telemetry.framework_logger', $config['framework_logger'] ?? null);

        $tracers = ($config['tracers'] ?? []) + ['default' => []];
        $meters = ($config['meters'] ?? []) + ['default' => []];
        $loggers = ($config['loggers'] ?? []) + ['default' => []];

        $this->registerGlobalServices($config, $container);
        $this->registerErrorHandlers($config['error_handlers'] ?? [], $container);
        $this->registerPropagator($config['propagator'] ?? [], $container);
        $this->registerResource($config['resource'], $container);
        $this->registerNamedExporters($config['exporters'] ?? [], $container);
        $this->registerTelemetry($config, $container);
        $this->registerInstrumentation($config['instrumentation'] ?? [], $container, $loader);
        $this->registerTracers($tracers, $container);
        $this->registerMeters($meters, $container);
        $this->registerLoggers($loggers, $container);
    }

    /**
     * @param array<string, mixed> $transportConfig
     */
    private function buildEmbeddedOtlpTransport(
        string $exporterName,
        array $transportConfig,
        ContainerBuilder $container,
        bool $allowFailover = true,
    ): string {
        $transportServiceId = 'flow.telemetry.exporter.' . $exporterName . '.transport';
        $type = $transportConfig['type'] ?? 'curl';

        if ($type === 'service') {
            $customServiceId = $transportConfig['service_id'] ?? null;

            if ($customServiceId === null) {
                throw new RuntimeException(sprintf(
                    'service_id is required when exporter "%s" transport type is "service"',
                    $exporterName,
                ));
            }
            $container->setAlias($transportServiceId, $customServiceId);

            return $transportServiceId;
        }

        $endpoint = $transportConfig['endpoint'] ?? null;

        if (!is_string($endpoint) || $endpoint === '') {
            throw new RuntimeException(sprintf('exporter "%s" transport requires an endpoint', $exporterName));
        }

        if ($type === 'stream') {
            $definition = new Definition(StreamTransport::class);
            $definition->setArgument(0, $endpoint);
            $definition->setArgument(1, $transportConfig['file_permissions'] ?? 0644);
            $definition->setArgument(2, $transportConfig['create_directories'] ?? true);
            $container->setDefinition($transportServiceId, $definition);

            return $transportServiceId;
        }

        switch ($type) {
            case 'curl':
                $optionsServiceId = $transportServiceId . '.options';
                $optionsDefinition = new Definition(CurlTransportOptions::class);
                $optionsDefinition->addMethodCall('withTimeout', [
                    $transportConfig['timeout_ms'] ?? CurlTransportOptions::DEFAULT_TIMEOUT_MS,
                ]);
                $optionsDefinition->addMethodCall('withConnectTimeout', [
                    $transportConfig['connect_timeout_ms'] ?? CurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS,
                ]);
                $optionsDefinition->addMethodCall('withShutdownTimeout', [
                    $transportConfig['shutdown_timeout_ms'] ?? CurlTransportOptions::DEFAULT_SHUTDOWN_TIMEOUT_MS,
                ]);

                $headers = $transportConfig['headers'] ?? [];

                foreach ($headers as $headerName => $headerValue) {
                    $optionsDefinition->addMethodCall('withHeader', [(string) $headerName, (string) $headerValue]);
                }

                if ($transportConfig['compression'] ?? false) {
                    $optionsDefinition->addMethodCall('withCompression', [true]);
                }

                $optionsDefinition->addMethodCall('withFollowRedirects', [
                    $transportConfig['follow_redirects'] ?? true,
                    $transportConfig['max_redirects'] ?? 3,
                ]);

                if (($transportConfig['proxy'] ?? null) !== null) {
                    $optionsDefinition->addMethodCall('withProxy', [$transportConfig['proxy']]);
                }

                $optionsDefinition->addMethodCall('withSslVerification', [
                    $transportConfig['ssl_verify_peer'] ?? true,
                    $transportConfig['ssl_verify_host'] ?? true,
                ]);

                if (($transportConfig['ssl_cert_path'] ?? null) !== null) {
                    $optionsDefinition->addMethodCall('withSslCertificate', [
                        $transportConfig['ssl_cert_path'],
                        $transportConfig['ssl_key_path'] ?? null,
                    ]);
                }

                if (($transportConfig['ca_info_path'] ?? null) !== null) {
                    $optionsDefinition->addMethodCall('withCaInfo', [$transportConfig['ca_info_path']]);
                }

                $container->setDefinition($optionsServiceId, $optionsDefinition);

                $serializerClass = match ($transportConfig['encoding'] ?? 'json') {
                    'protobuf' => ProtobufSerializer::class,
                    default => JsonSerializer::class,
                };

                $definition = new Definition(CurlTransport::class);
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Definition($serializerClass));
                $definition->setArgument(2, new Reference($optionsServiceId));

                $failoverReference = $this->buildFailoverTransport(
                    $exporterName,
                    $transportConfig,
                    $container,
                    $allowFailover,
                );

                if ($failoverReference !== null) {
                    $definition->setArgument(3, $failoverReference);
                }

                $container->setDefinition($transportServiceId, $definition);

                break;

            case 'grpc':
                $definition = new Definition(GrpcTransport::class);
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, $transportConfig['headers'] ?? []);
                $definition->setArgument(2, $transportConfig['insecure'] ?? true);
                $definition->setArgument(3, $transportConfig['timeout_ms'] ?? GrpcTransport::DEFAULT_TIMEOUT_MS);
                $definition->setArgument(
                    4,
                    $transportConfig['shutdown_timeout_ms'] ?? GrpcTransport::DEFAULT_SHUTDOWN_TIMEOUT_MS,
                );

                $failoverReference = $this->buildFailoverTransport(
                    $exporterName,
                    $transportConfig,
                    $container,
                    $allowFailover,
                );

                if ($failoverReference !== null) {
                    $definition->setArgument(5, $failoverReference);
                }

                $container->setDefinition($transportServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf(
                    'Unknown transport type "%s" for exporter "%s"',
                    (string) $type,
                    $exporterName,
                ));
        }

        return $transportServiceId;
    }

    /**
     * @param array<string, mixed> $handlerConfig
     */
    private function buildErrorHandlerDefinition(string $name, array $handlerConfig, ContainerBuilder $container): void
    {
        $serviceId = 'flow.telemetry.error_handler.' . $name;
        $type = $handlerConfig['type'] ?? 'error_log';

        switch ($type) {
            case 'error_log':
                $definition = new Definition(ErrorLogHandler::class);
                $definition->setArgument(
                    0,
                    $this->mapErrorLogMessageType($handlerConfig['message_type'] ?? 'operating_system'),
                );
                $definition->setArgument(1, $handlerConfig['expand_newlines'] ?? false);
                $definition->setArgument(2, $handlerConfig['message_prefix'] ?? '[flow-telemetry]');
                $container->setDefinition($serviceId, $definition);

                break;

            case 'stream':
                $destination = $handlerConfig['destination'] ?? null;

                if (!is_string($destination) || $destination === '') {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "stream" requires a non-empty "destination"',
                        $name,
                    ));
                }
                $definition = new Definition(StreamHandler::class);
                $definition->setArgument(0, $destination);
                $definition->setArgument(1, $handlerConfig['file_permissions'] ?? 0644);
                $definition->setArgument(2, $handlerConfig['create_directories'] ?? true);
                $definition->setArgument(3, $handlerConfig['message_prefix'] ?? '[flow-telemetry]');
                $container->setDefinition($serviceId, $definition);

                break;

            case 'syslog':
                $definition = new Definition(SyslogHandler::class);
                $definition->setArgument(0, $handlerConfig['ident'] ?? 'flow-telemetry');
                $definition->setArgument(1, $this->mapSyslogFacility($handlerConfig['facility'] ?? 'user'));
                $definition->setArgument(2, $handlerConfig['log_opts'] ?? LOG_PID);
                $definition->setArgument(3, $this->mapSyslogSeverity($handlerConfig['severity'] ?? 'error'));
                $container->setDefinition($serviceId, $definition);

                break;

            case 'udp_syslog':
                $host = $handlerConfig['host'] ?? null;

                if (!is_string($host) || $host === '') {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "udp_syslog" requires a non-empty "host"',
                        $name,
                    ));
                }
                $definition = new Definition(UdpSyslogHandler::class);
                $definition->setArgument(0, $host);
                $definition->setArgument(1, $handlerConfig['port'] ?? 514);
                $definition->setArgument(2, $handlerConfig['ident'] ?? 'flow-telemetry');
                $definition->setArgument(3, $this->mapSyslogFacility($handlerConfig['facility'] ?? 'user'));
                $definition->setArgument(4, $this->mapSyslogSeverity($handlerConfig['severity'] ?? 'error'));
                $container->setDefinition($serviceId, $definition);

                break;

            case 'composite':
                $children = $handlerConfig['handlers'] ?? [];

                if (!is_array($children) || count($children) === 0) {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "composite" requires a non-empty "handlers" list',
                        $name,
                    ));
                }
                $childRefs = [];

                foreach ($children as $childName) {
                    $childRefs[] = $this->resolveErrorHandlerReference($childName, $container);
                }
                $container->setDefinition($serviceId, new Definition(CompositeErrorHandler::class, $childRefs));

                break;

            case 'noop':
                $container->setDefinition($serviceId, new Definition(NullErrorHandler::class));

                break;

            case 'service':
                $customServiceId = $handlerConfig['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "service" requires a non-empty "service_id"',
                        $name,
                    ));
                }
                $container->setAlias($serviceId, $customServiceId);

                break;

            default:
                throw new RuntimeException(sprintf(
                    'Unknown error_handler type "%s" for handler "%s"',
                    (string) $type,
                    $name,
                ));
        }
    }

    /**
     * @param array<string, mixed> $transportConfig
     */
    private function buildFailoverTransport(
        string $exporterName,
        array $transportConfig,
        ContainerBuilder $container,
        bool $allowFailover,
    ): ?Reference {
        if (!$allowFailover) {
            return null;
        }

        $failoverConfig = $transportConfig['failover'] ?? null;

        if (!is_array($failoverConfig) || $failoverConfig === []) {
            return null;
        }

        $failoverServiceId = $this->buildEmbeddedOtlpTransport(
            $exporterName . '.failover',
            $failoverConfig,
            $container,
            allowFailover: false,
        );

        return new Reference($failoverServiceId);
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildLoggerProvider(array $config, ContainerBuilder $container): string
    {
        $providerServiceId = 'flow.telemetry.logger_provider';

        $processorServiceId = $this->buildLogProcessor($config['processor'] ?? [], $providerServiceId, $container);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

        $definition = new Definition(LoggerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildLogProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

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
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $container);
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $container);
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $container);
                $definition = new Definition(PassThroughLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
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
                        $container,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeLogProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'severity_filtering':
                $innerProcessorConfig = $config['inner_processor'] ?? [];
                $innerProcessorServiceId = $this->buildLogProcessor(
                    $innerProcessorConfig,
                    $processorServiceId . '.inner',
                    $container,
                );
                $minimumSeverity = $this->mapSeverity($config['minimum_severity'] ?? 'info');
                $definition = new Definition(SeverityFilteringLogProcessor::class);
                $definition->setArgument(0, new Reference($innerProcessorServiceId));
                $definition->setArgument(1, $minimumSeverity);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown log processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildMeterProvider(array $config, ContainerBuilder $container): string
    {
        $providerServiceId = 'flow.telemetry.meter_provider';

        $processorServiceId = $this->buildMetricProcessor($config['processor'] ?? [], $providerServiceId, $container);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

        $temporality = ($config['temporality'] ?? 'cumulative') === 'delta'
            ? AggregationTemporality::DELTA
            : AggregationTemporality::CUMULATIVE;

        $definition = new Definition(MeterProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, $temporality);
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildMetricProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

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
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $container);
                $definition = new Definition(MemoryMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $container);
                $definition = new Definition(BatchingMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $container);
                $definition = new Definition(PassThroughMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
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
                        $container,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeMetricProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown metric processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildSampler(array $config, ContainerBuilder $container): string
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
                throw new RuntimeException(sprintf('Unknown sampler type: %s', (string) $type));
        }

        return $samplerServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildSpanProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $container): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

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
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $container);
                $definition = new Definition(MemorySpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $container);
                $definition = new Definition(BatchingSpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $container);
                $definition = new Definition(PassThroughSpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
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
                        $container,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeSpanProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown span processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<string, mixed> $config
     */
    private function buildTracerProvider(array $config, ContainerBuilder $container): string
    {
        $providerServiceId = 'flow.telemetry.tracer_provider';

        $processorServiceId = $this->buildSpanProcessor($config['processor'] ?? [], $providerServiceId, $container);
        $samplerServiceId = $this->buildSampler($config['sampler'] ?? [], $container);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $container);

        $definition = new Definition(TracerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $definition->setArgument(3, new Reference($samplerServiceId));
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $container->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    private function mapErrorLogMessageType(string $value): ErrorLogMessageType
    {
        return match ($value) {
            'operating_system' => ErrorLogMessageType::OperatingSystem,
            'email' => ErrorLogMessageType::Email,
            'file' => ErrorLogMessageType::File,
            'sapi' => ErrorLogMessageType::Sapi,
            default => throw new RuntimeException(sprintf('Unknown error_log message_type: %s', $value)),
        };
    }

    private function mapSeverity(string $severity): Severity
    {
        return match ($severity) {
            'trace' => Severity::TRACE,
            'debug' => Severity::DEBUG,
            'info' => Severity::INFO,
            'warn' => Severity::WARN,
            'error' => Severity::ERROR,
            'fatal' => Severity::FATAL,
            default => throw new RuntimeException(sprintf('Unknown severity level: %s', $severity)),
        };
    }

    private function mapSyslogFacility(string $value): SyslogFacility
    {
        return match ($value) {
            'auth' => SyslogFacility::Auth,
            'cron' => SyslogFacility::Cron,
            'daemon' => SyslogFacility::Daemon,
            'kernel' => SyslogFacility::Kernel,
            'local0' => SyslogFacility::Local0,
            'local1' => SyslogFacility::Local1,
            'local2' => SyslogFacility::Local2,
            'local3' => SyslogFacility::Local3,
            'local4' => SyslogFacility::Local4,
            'local5' => SyslogFacility::Local5,
            'local6' => SyslogFacility::Local6,
            'local7' => SyslogFacility::Local7,
            'lpr' => SyslogFacility::Lpr,
            'mail' => SyslogFacility::Mail,
            'news' => SyslogFacility::News,
            'syslog' => SyslogFacility::Syslog,
            'user' => SyslogFacility::User,
            'uucp' => SyslogFacility::Uucp,
            default => throw new RuntimeException(sprintf('Unknown syslog facility: %s', $value)),
        };
    }

    private function mapSyslogSeverity(string $value): SyslogSeverity
    {
        return match ($value) {
            'alert' => SyslogSeverity::Alert,
            'critical' => SyslogSeverity::Critical,
            'debug' => SyslogSeverity::Debug,
            'emergency' => SyslogSeverity::Emergency,
            'error' => SyslogSeverity::Error,
            'info' => SyslogSeverity::Info,
            'notice' => SyslogSeverity::Notice,
            'warning' => SyslogSeverity::Warning,
            default => throw new RuntimeException(sprintf('Unknown syslog severity: %s', $value)),
        };
    }

    /**
     * @param array<string, mixed> $config
     */
    private function readConfigEnabled(string $path, ContainerBuilder $container, array $config): bool
    {
        return $this->configsEnabled[$path] ??= parent::isConfigEnabled($container, $config);
    }

    /**
     * @param array<string, array<string, mixed>> $config
     */
    private function registerErrorHandlers(array $config, ContainerBuilder $container): void
    {
        if (!array_key_exists('default', $config)) {
            $config = ['default' => ['type' => 'error_log']] + $config;
        }

        $compositeNames = [];

        foreach ($config as $name => $handlerConfig) {
            $type = $handlerConfig['type'] ?? 'error_log';

            if ($type === 'composite') {
                $compositeNames[] = $name;

                continue;
            }

            $this->buildErrorHandlerDefinition((string) $name, $handlerConfig, $container);
        }

        foreach ($compositeNames as $name) {
            $this->buildErrorHandlerDefinition((string) $name, $config[$name], $container);
        }
    }

    /**
     * @param array<string, mixed> $config
     */
    private function registerGlobalServices(array $config, ContainerBuilder $container): void
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
            new Definition(LogRecordConverter::class),
        );
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}} $config
     */
    private function registerInstrumentation(array $config, ContainerBuilder $container, PhpFileLoader $loader): void
    {
        $httpKernelConfig = $config['http_kernel'] ?? [];

        if ($this->readConfigEnabled('instrumentation.http_kernel', $container, $httpKernelConfig)) {
            $container->setParameter(
                'flow.telemetry.http_kernel.exclude_paths',
                $httpKernelConfig['exclude_paths'] ?? [],
            );
            $container->setParameter(
                'flow.telemetry.http_kernel.context_propagation',
                $httpKernelConfig['context_propagation'] ?? true,
            );
            $loader->load('instrumentation/http_kernel.php');
        }

        $consoleConfig = $config['console'] ?? [];

        if ($this->readConfigEnabled('instrumentation.console', $container, $consoleConfig)) {
            $container->setParameter(
                'flow.telemetry.console.exclude_commands',
                $consoleConfig['exclude_commands'] ?? [],
            );
            $loader->load('instrumentation/console.php');
        }

        $messengerConfig = $config['messenger'] ?? [];

        if ($this->readConfigEnabled('instrumentation.messenger', $container, $messengerConfig)) {
            if (!interface_exists(self::MESSENGER_MIDDLEWARE_INTERFACE)) {
                throw new RuntimeException(
                    'Messenger instrumentation requires symfony/messenger package. Install it via composer: composer require symfony/messenger',
                );
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
            if (!class_exists(AbstractExtension::class)) {
                throw new RuntimeException(
                    'Twig instrumentation requires twig/twig package. Install it via composer: composer require twig/twig',
                );
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
    private function registerLoggers(array $config, ContainerBuilder $container): void
    {
        foreach ($config as $name => $loggerConfig) {
            $definition = new Definition(Logger::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'logger']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $loggerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $loggerConfig['schema_url'] ?? null);

            $attributes = $loggerConfig['attributes'] ?? [];

            if (count($attributes) > 0) {
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
    private function registerMeters(array $config, ContainerBuilder $container): void
    {
        foreach ($config as $name => $meterConfig) {
            $definition = new Definition(Meter::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'meter']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $meterConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $meterConfig['schema_url'] ?? null);

            $attributes = $meterConfig['attributes'] ?? [];

            if (count($attributes) > 0) {
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
     * @param array<string, array<string, mixed>> $config
     */
    private function registerNamedExporters(array $config, ContainerBuilder $container): void
    {
        foreach ($config as $name => $exporterConfig) {
            $serviceId = 'flow.telemetry.exporter.' . $name;

            if (array_key_exists('void', $exporterConfig)) {
                $container->setDefinition($serviceId, new Definition(VoidExporter::class));

                continue;
            }

            if (array_key_exists('memory', $exporterConfig)) {
                $container->setDefinition($serviceId, new Definition(MemoryExporter::class));

                continue;
            }

            if (array_key_exists('console', $exporterConfig)) {
                $container->setDefinition($serviceId, new Definition(ConsoleExporter::class));

                continue;
            }

            if (array_key_exists('service', $exporterConfig)) {
                $customServiceId = $exporterConfig['service']['id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException(sprintf('exporter "%s" of type "service" requires "service.id"', $name));
                }
                $container->setAlias($serviceId, $customServiceId);

                continue;
            }

            if (array_key_exists('otlp', $exporterConfig)) {
                $container->setParameter('flow.telemetry.otlp_configured', true);
                $transportConfig = $exporterConfig['otlp']['transport'] ?? null;

                if (!is_array($transportConfig) || count($transportConfig) === 0) {
                    throw new RuntimeException(sprintf(
                        'exporter "%s" of type "otlp" requires an inline "transport" configuration',
                        $name,
                    ));
                }
                $transportServiceId = $this->buildEmbeddedOtlpTransport($name, $transportConfig, $container);
                $errorHandlerRef = $this->resolveErrorHandlerReference(
                    $exporterConfig['otlp']['error_handler'] ?? 'default',
                    $container,
                );
                $definition = new Definition(OTLPExporter::class);
                $definition->setArgument(0, new Reference($transportServiceId));
                $definition->setArgument(1, $errorHandlerRef);
                $container->setDefinition($serviceId, $definition);

                continue;
            }

            throw new RuntimeException(sprintf(
                'exporter "%s" must declare exactly one of: otlp, service, console, memory, void',
                $name,
            ));
        }
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, log_sql?: bool, max_sql_length?: int, exclude_connections?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>}} $config
     */
    private function registerParameterOnlyInstrumentation(array $config, ContainerBuilder $container): void
    {
        $httpClientConfig = $config['http_client'] ?? [];
        $container->setParameter('flow.telemetry.http_client.enabled', $httpClientConfig['enabled'] ?? false);
        $container->setParameter(
            'flow.telemetry.http_client.exclude_clients',
            $httpClientConfig['exclude_clients'] ?? [],
        );

        $psr18ClientConfig = $config['psr18_client'] ?? [];
        $container->setParameter('flow.telemetry.psr18_client.enabled', $psr18ClientConfig['enabled'] ?? false);
        $container->setParameter(
            'flow.telemetry.psr18_client.exclude_clients',
            $psr18ClientConfig['exclude_clients'] ?? [],
        );

        $dbalConfig = $config['dbal'] ?? [];
        $container->setParameter('flow.telemetry.dbal.enabled', $dbalConfig['enabled'] ?? false);
        $container->setParameter('flow.telemetry.dbal.log_sql', $dbalConfig['log_sql'] ?? true);
        $container->setParameter('flow.telemetry.dbal.max_sql_length', $dbalConfig['max_sql_length'] ?? 1000);
        $container->setParameter('flow.telemetry.dbal.exclude_connections', $dbalConfig['exclude_connections'] ?? []);

        $cacheConfig = $config['cache'] ?? [];
        $container->setParameter('flow.telemetry.cache.enabled', $cacheConfig['enabled'] ?? false);
        $container->setParameter('flow.telemetry.cache.exclude_pools', $cacheConfig['exclude_pools'] ?? []);
    }

    /**
     * @param array{type?: string, service_id?: null|string} $config
     */
    private function registerPropagator(array $config, ContainerBuilder $container): void
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
                $container->setDefinition(
                    'flow.telemetry.propagator.tracecontext',
                    new Definition(W3CTraceContext::class),
                );
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
                throw new RuntimeException(sprintf('Unknown propagator type: %s', (string) $type));
        }
    }

    /**
     * @param array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>} $resourceConfig
     */
    private function registerResource(array $resourceConfig, ContainerBuilder $container): void
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
            $container->setDefinition('flow.telemetry.resource.detector.os', new Definition(OsDetector::class));
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.os');
        }

        if ($staticConfig['host']['enabled'] ?? true) {
            $container->setDefinition('flow.telemetry.resource.detector.host', new Definition(HostDetector::class));
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

        if (count($customAttributes) > 0) {
            $manualDefinition = new Definition(ManualDetector::class);
            $manualDefinition->setArgument(0, $customAttributes);
            $container->setDefinition('flow.telemetry.resource.detector.custom', $manualDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.custom');
        }

        if ($staticConfig['environment']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.environment',
                new Definition(EnvironmentDetector::class),
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
            $container->setAlias(
                'flow.telemetry.resource.detector.static',
                'flow.telemetry.resource.detector.static.chain',
            );
        }

        $dynamicDetectorRefs = [];

        if ($dynamicConfig['process']['enabled'] ?? true) {
            $container->setDefinition(
                'flow.telemetry.resource.detector.process',
                new Definition(ProcessDetector::class),
            );
            $dynamicDetectorRefs[] = new Reference('flow.telemetry.resource.detector.process');
        }

        if (count($dynamicDetectorRefs) > 0) {
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
    private function registerTelemetry(array $config, ContainerBuilder $container): void
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
    private function registerTracers(array $config, ContainerBuilder $container): void
    {
        foreach ($config as $name => $tracerConfig) {
            $definition = new Definition(Tracer::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'tracer']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $tracerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $tracerConfig['schema_url'] ?? null);

            $attributes = $tracerConfig['attributes'] ?? [];

            if (count($attributes) > 0) {
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

    private function resolveErrorHandlerReference(mixed $name, ContainerBuilder $container): Reference
    {
        if (!is_string($name) || $name === '') {
            $name = 'default';
        }

        $serviceId = 'flow.telemetry.error_handler.' . $name;

        if (!$container->hasDefinition($serviceId) && !$container->hasAlias($serviceId)) {
            throw new RuntimeException(sprintf(
                'Unknown error_handler "%s"; declare it under flow_telemetry.error_handlers',
                $name,
            ));
        }

        return new Reference($serviceId);
    }

    private function resolveExporterReference(
        string $signalLabel,
        mixed $exporterName,
        ContainerBuilder $container,
    ): Reference {
        if (!is_string($exporterName) || $exporterName === '') {
            throw new RuntimeException(sprintf(
                'Missing "exporter" reference for %s processor; expected a name from top-level "exporters"',
                $signalLabel,
            ));
        }

        $serviceId = 'flow.telemetry.exporter.' . $exporterName;

        if (!$container->hasDefinition($serviceId) && !$container->hasAlias($serviceId)) {
            throw new RuntimeException(sprintf(
                '%s processor references unknown exporter "%s"',
                ucfirst($signalLabel),
                $exporterName,
            ));
        }

        return new Reference($serviceId);
    }
}
