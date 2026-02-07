<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\{ArrayNodeDefinition, TreeBuilder};
use Symfony\Component\Config\Definition\ConfigurationInterface;

final class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder() : TreeBuilder
    {
        $treeBuilder = new TreeBuilder('flow_telemetry');
        $rootNode = $treeBuilder->getRootNode();

        $rootNode
            ->children()
                ->arrayNode('service')
                    ->info('Service resource configuration')
                    ->isRequired()
                    ->children()
                        ->scalarNode('name')
                            ->info('Service name for Resource (e.g., "MyApp")')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('version')
                            ->info('Service version (e.g., "1.0.0")')
                            ->defaultNull()
                        ->end()
                        ->arrayNode('attributes')
                            ->info('Additional resource attributes')
                            ->normalizeKeys(false)
                            ->useAttributeAsKey('name')
                            ->prototype('variable')->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('tracer_provider')
                    ->info('TracerProvider configuration. Defaults to void if omitted.')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->arrayNode('sampler')
                            ->info('Trace sampler configuration')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->enumNode('type')
                                    ->values(['always_on', 'always_off', 'trace_id_ratio', 'parent_based', 'service'])
                                    ->defaultValue('always_on')
                                ->end()
                                ->floatNode('ratio')
                                    ->info('Sampling ratio for trace_id_ratio type (0.0 to 1.0)')
                                    ->defaultValue(1.0)
                                    ->min(0.0)
                                    ->max(1.0)
                                ->end()
                                ->scalarNode('service_id')
                                    ->info('Custom sampler service ID (only for type: service)')
                                    ->defaultNull()
                                ->end()
                            ->end()
                        ->end()
                        ->append($this->processorNode('span'))
                    ->end()
                ->end()
                ->arrayNode('meter_provider')
                    ->info('MeterProvider configuration. Defaults to void if omitted.')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->enumNode('temporality')
                            ->info('Aggregation temporality')
                            ->values(['cumulative', 'delta'])
                            ->defaultValue('cumulative')
                        ->end()
                        ->append($this->processorNode('metric'))
                    ->end()
                ->end()
                ->arrayNode('logger_provider')
                    ->info('LoggerProvider configuration. Defaults to void if omitted.')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->append($this->processorNode('log'))
                    ->end()
                ->end()
                ->arrayNode('instrumentation')
                    ->info('Auto-instrumentation configuration')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->booleanNode('http_kernel')
                            ->info('Enable automatic tracing of HTTP requests')
                            ->defaultFalse()
                        ->end()
                        ->booleanNode('console')
                            ->info('Enable automatic tracing of console commands')
                            ->defaultFalse()
                        ->end()
                        ->booleanNode('messenger')
                            ->info('Enable automatic tracing of Messenger messages')
                            ->defaultFalse()
                        ->end()
                    ->end()
                ->end()
            ->end();

        return $treeBuilder;
    }

    private function exporterNode(string $signalType) : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('exporter');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info(\ucfirst($signalType) . ' exporter configuration')
            ->addDefaultsIfNotSet()
            ->children()
                ->enumNode('type')
                    ->values(['memory', 'console', 'void', 'otlp', 'service'])
                    ->defaultValue('void')
                ->end()
                ->scalarNode('service_id')
                    ->info('Custom exporter service ID (only for type: service)')
                    ->defaultNull()
                ->end()
                ->arrayNode('otlp')
                    ->info('OTLP exporter configuration (only for type: otlp)')
                    ->children()
                        ->arrayNode('transport')
                            ->info('OTLP transport configuration')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->enumNode('type')
                                    ->values(['curl', 'http', 'grpc', 'service'])
                                    ->defaultValue('curl')
                                ->end()
                                ->scalarNode('endpoint')
                                    ->info('OTLP endpoint URL')
                                    ->defaultValue('http://localhost:4318')
                                ->end()
                                ->integerNode('timeout')
                                    ->info('Request timeout in seconds')
                                    ->defaultValue(30)
                                    ->min(1)
                                ->end()
                                ->arrayNode('headers')
                                    ->info('Additional HTTP headers')
                                    ->normalizeKeys(false)
                                    ->useAttributeAsKey('name')
                                    ->prototype('scalar')->end()
                                ->end()
                                ->booleanNode('insecure')
                                    ->info('Allow insecure connections (only for grpc)')
                                    ->defaultTrue()
                                ->end()
                                ->scalarNode('service_id')
                                    ->info('Custom transport service ID (only for type: service)')
                                    ->defaultNull()
                                ->end()
                                ->arrayNode('serializer')
                                    ->info('Serializer configuration')
                                    ->addDefaultsIfNotSet()
                                    ->children()
                                        ->enumNode('type')
                                            ->values(['json', 'protobuf', 'service'])
                                            ->defaultValue('json')
                                        ->end()
                                        ->scalarNode('service_id')
                                            ->info('Custom serializer service ID (only for type: service)')
                                            ->defaultNull()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();

        return $node;
    }

    private function innerProcessorNode(string $signalType) : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('inner_processor');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $childProcessorTypes = ['memory', 'batching', 'passthrough', 'void', 'service'];

        $node
            ->info('Inner processor configuration for severity_filtering (only for type: severity_filtering)')
            ->children()
                ->enumNode('type')
                    ->values($childProcessorTypes)
                    ->isRequired()
                ->end()
                ->integerNode('batch_size')
                    ->info('Batch size for batching processor')
                    ->defaultValue(512)
                    ->min(1)
                ->end()
                ->scalarNode('service_id')
                    ->info('Custom processor service ID (only for type: service)')
                    ->defaultNull()
                ->end()
                ->append($this->exporterNode($signalType))
            ->end();

        return $node;
    }

    private function processorNode(string $signalType) : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('processor');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $processorTypes = ['composite', 'memory', 'batching', 'passthrough', 'void', 'service'];
        $childProcessorTypes = ['memory', 'batching', 'passthrough', 'void', 'service'];

        if ($signalType === 'log') {
            $processorTypes[] = 'severity_filtering';
            $childProcessorTypes[] = 'severity_filtering';
        }

        $node
            ->info(\ucfirst($signalType) . ' processor configuration')
            ->addDefaultsIfNotSet()
            ->children()
                ->enumNode('type')
                    ->values($processorTypes)
                    ->defaultValue('void')
                ->end()
                ->integerNode('batch_size')
                    ->info('Batch size for batching processor')
                    ->defaultValue(512)
                    ->min(1)
                ->end()
                ->scalarNode('service_id')
                    ->info('Custom processor service ID (only for type: service)')
                    ->defaultNull()
                ->end()
                ->enumNode('minimum_severity')
                    ->info('Minimum severity level for severity_filtering processor (only for log processors)')
                    ->values(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
                    ->defaultValue('info')
                ->end()
                ->arrayNode('processors')
                    ->info('Array of processor configurations (only for type: composite)')
                    ->arrayPrototype()
                        ->children()
                            ->enumNode('type')
                                ->values($childProcessorTypes)
                                ->isRequired()
                            ->end()
                            ->integerNode('batch_size')
                                ->defaultValue(512)
                                ->min(1)
                            ->end()
                            ->scalarNode('service_id')
                                ->defaultNull()
                            ->end()
                            ->enumNode('minimum_severity')
                                ->values(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
                                ->defaultValue('info')
                            ->end()
                            ->append($this->exporterNode($signalType))
                            ->append($this->innerProcessorNode($signalType))
                        ->end()
                    ->end()
                ->end()
                ->append($this->exporterNode($signalType))
                ->append($this->innerProcessorNode($signalType))
            ->end();

        return $node;
    }
}
