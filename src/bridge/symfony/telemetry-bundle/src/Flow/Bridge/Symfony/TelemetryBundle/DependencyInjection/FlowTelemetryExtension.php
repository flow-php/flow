<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{LoggerProvider, Severity};
use Flow\Telemetry\Logger\Processor\{BatchingLogProcessor, CompositeLogProcessor, PassThroughLogProcessor, SeverityFilteringLogProcessor};
use Flow\Telemetry\Meter\{AggregationTemporality, MeterProvider};
use Flow\Telemetry\Meter\Processor\{BatchingMetricProcessor, CompositeMetricProcessor, PassThroughMetricProcessor};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Console\{ConsoleLogExporter, ConsoleMetricExporter, ConsoleSpanExporter};
use Flow\Telemetry\Provider\Memory\{MemoryLogExporter, MemoryLogProcessor, MemoryMetricExporter, MemoryMetricProcessor, MemorySpanExporter, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidLogProcessor, VoidMetricExporter, VoidMetricProcessor, VoidSpanExporter, VoidSpanProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\Processor\{BatchingSpanProcessor, CompositeSpanProcessor, PassThroughSpanProcessor};
use Flow\Telemetry\Tracer\Sampler\{AlwaysOffSampler, AlwaysOnSampler, ParentBasedSampler, TraceIdRatioBasedSampler};
use Flow\Telemetry\Tracer\TracerProvider;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};
use Symfony\Component\DependencyInjection\Extension\Extension;

final class FlowTelemetryExtension extends Extension
{
    /**
     * @param array<array-key, mixed> $configs
     */
    public function load(array $configs, ContainerBuilder $container) : void
    {
        $configuration = new Configuration();
        /** @var array{service: array<string, mixed>, instances?: array<string, array<string, mixed>>} $config */
        $config = $this->processConfiguration($configuration, $configs);

        $this->registerGlobalServices($container);
        $this->registerResource($config['service'], $container);
        $this->registerInstances($config['instances'] ?? [], $container);
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
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
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
                $transportServiceId = $this->buildOTLPTransport($config['otlp']['transport'] ?? [], $exporterServiceId, $container);
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\LogExporter\\OTLPLogExporter');
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
    private function buildLoggerProvider(array $config, string $instanceName, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.' . $instanceName . '.logger_provider';

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
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildLogExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(PassThroughLogProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildLogProcessor($processorConfig, $processorServiceId . '.' . $idx, $container);
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeLogProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'severity_filtering':
                $innerProcessorConfig = $config['inner_processor'] ?? [];
                $innerProcessorServiceId = $this->buildInnerLogProcessor($innerProcessorConfig, $processorServiceId . '.inner', $container);
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
    private function buildMeterProvider(array $config, string $instanceName, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.' . $instanceName . '.meter_provider';

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
                $transportServiceId = $this->buildOTLPTransport($config['otlp']['transport'] ?? [], $exporterServiceId, $container);
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\MetricExporter\\OTLPMetricExporter');
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
                $exporterServiceId = $this->buildMetricExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(MemoryMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildMetricExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(BatchingMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildMetricExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(PassThroughMetricProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildMetricProcessor($processorConfig, $processorServiceId . '.' . $idx, $container);
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
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\Serializer\\JsonSerializer');
                $container->setDefinition($serializerServiceId, $definition);

                break;

            case 'protobuf':
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\Serializer\\ProtobufSerializer');
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
        $type = $config['type'] ?? 'curl';

        if ($type === 'service') {
            $customServiceId = $config['service_id'] ?? null;

            if ($customServiceId === null) {
                throw new RuntimeException('service_id is required when transport type is "service"');
            }
            $container->setAlias($transportServiceId, $customServiceId);

            return $transportServiceId;
        }

        $endpoint = $config['endpoint'] ?? 'http://localhost:4318';
        $timeout = $config['timeout'] ?? 30;
        $headers = $config['headers'] ?? [];

        $serializerServiceId = $this->buildOTLPSerializer($config['serializer'] ?? [], $transportServiceId, $container);

        switch ($type) {
            case 'curl':
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\Transport\\CurlTransport');
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Reference($serializerServiceId));
                $definition->setArgument(2, $timeout);
                $definition->setArgument(3, $headers);
                $container->setDefinition($transportServiceId, $definition);

                break;

            case 'http':
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\Transport\\HttpTransport');
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Reference($serializerServiceId));
                $definition->setArgument(2, $timeout);
                $definition->setArgument(3, $headers);
                $container->setDefinition($transportServiceId, $definition);

                break;

            case 'grpc':
                $insecure = $config['insecure'] ?? true;
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\Transport\\GrpcTransport');
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Reference($serializerServiceId));
                $definition->setArgument(2, $timeout);
                $definition->setArgument(3, $headers);
                $definition->setArgument(4, $insecure);
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
    private function buildSampler(array $config, string $instanceName, ContainerBuilder $container) : string
    {
        $samplerServiceId = 'flow.telemetry.' . $instanceName . '.tracer_provider.sampler';
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
                $transportServiceId = $this->buildOTLPTransport($config['otlp']['transport'] ?? [], $exporterServiceId, $container);
                $definition = new Definition('Flow\\Bridge\\Telemetry\\OTLP\\SpanExporter\\OTLPSpanExporter');
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
                $exporterServiceId = $this->buildSpanExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(MemorySpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterServiceId = $this->buildSpanExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(BatchingSpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterServiceId = $this->buildSpanExporter($config['exporter'] ?? [], $processorServiceId, $container);
                $definition = new Definition(PassThroughSpanProcessor::class);
                $definition->setArgument(0, new Reference($exporterServiceId));
                $container->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = $config['processors'] ?? [];
                $processorRefs = [];

                foreach ($processors as $idx => $processorConfig) {
                    /** @var array<string, mixed> $processorConfig */
                    $subProcessorId = $this->buildSpanProcessor($processorConfig, $processorServiceId . '.' . $idx, $container);
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
    private function buildTracerProvider(array $config, string $instanceName, ContainerBuilder $container) : string
    {
        $providerServiceId = 'flow.telemetry.' . $instanceName . '.tracer_provider';

        $processorServiceId = $this->buildSpanProcessor($config['processor'] ?? [], $providerServiceId, $container);
        $samplerServiceId = $this->buildSampler($config['sampler'] ?? [], $instanceName, $container);

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

    private function registerGlobalServices(ContainerBuilder $container) : void
    {
        $container->setDefinition('flow.telemetry.clock', new Definition(SystemClock::class));
        $container->setDefinition('flow.telemetry.context_storage', new Definition(MemoryContextStorage::class));
    }

    /**
     * @param array<string, array<string, mixed>> $instances
     */
    private function registerInstances(array $instances, ContainerBuilder $container) : void
    {
        if (\count($instances) === 0) {
            $instances = ['default' => []];
        }

        foreach ($instances as $name => $config) {
            $this->registerTelemetryInstance($name, $config, $container);
        }
    }

    /**
     * @param array<string, mixed> $serviceConfig
     */
    private function registerResource(array $serviceConfig, ContainerBuilder $container) : void
    {
        $attributes = [
            'service.name' => $serviceConfig['name'],
        ];

        if (isset($serviceConfig['version']) && $serviceConfig['version'] !== null) {
            $attributes['service.version'] = $serviceConfig['version'];
        }

        $additionalAttributes = $serviceConfig['attributes'] ?? [];

        foreach ($additionalAttributes as $key => $value) {
            $attributes[(string) $key] = $value;
        }

        $definition = new Definition(Resource::class);
        $definition->setFactory([Resource::class, 'create']);
        $definition->setArgument(0, $attributes);
        $container->setDefinition('flow.telemetry.resource', $definition);
    }

    /**
     * @param array<string, mixed> $config
     */
    private function registerTelemetryInstance(string $name, array $config, ContainerBuilder $container) : void
    {
        $tracerProviderServiceId = $this->buildTracerProvider($config['tracer_provider'] ?? [], $name, $container);
        $meterProviderServiceId = $this->buildMeterProvider($config['meter_provider'] ?? [], $name, $container);
        $loggerProviderServiceId = $this->buildLoggerProvider($config['logger_provider'] ?? [], $name, $container);

        $telemetryServiceId = 'flow.telemetry.' . $name;
        $definition = new Definition(Telemetry::class);
        $definition->setArgument(0, new Reference('flow.telemetry.resource'));
        $definition->setArgument(1, new Reference($tracerProviderServiceId));
        $definition->setArgument(2, new Reference($meterProviderServiceId));
        $definition->setArgument(3, new Reference($loggerProviderServiceId));
        $definition->setPublic(true);
        $container->setDefinition($telemetryServiceId, $definition);

        if ($name === 'default') {
            $container->setAlias(Telemetry::class, $telemetryServiceId)->setPublic(true);
        }
    }
}
