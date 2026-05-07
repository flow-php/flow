<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\{ArrayNodeDefinition, TreeBuilder};
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

final class Configuration implements ConfigurationInterface
{
    public function getConfigTreeBuilder() : TreeBuilder
    {
        $treeBuilder = new TreeBuilder('flow_telemetry');
        $rootNode = $treeBuilder->getRootNode();

        $rootNode
            ->children()
                ->arrayNode('resource')
                    ->info('OpenTelemetry Resource configuration with automatic detection (https://opentelemetry.io/docs/specs/semconv/resource/)')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->arrayNode('detectors')
                            ->info('Resource detector configuration for automatic attribute detection')
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->booleanNode('enabled')
                                    ->info('Enable resource detectors (default: true)')
                                    ->defaultTrue()
                                ->end()
                                ->arrayNode('static')
                                    ->info('Static detectors - values that do not change between requests/commands (can be cached)')
                                    ->addDefaultsIfNotSet()
                                    ->children()
                                        ->arrayNode('cache')
                                            ->info('File-based caching of static resource attributes. The cache file is intentionally outside Symfony\'s cache lifecycle so build-time cache:warmup does not freeze runtime-dependent attributes (host, process).')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')
                                                    ->info('Enable caching of static resource attributes')
                                                    ->defaultTrue()
                                                ->end()
                                                ->scalarNode('path')
                                                    ->info('Absolute path to the cache file. Default: sys_get_temp_dir()/flow_telemetry_resource.cache.')
                                                    ->defaultNull()
                                                ->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('os')
                                            ->info('OS detector - detects os.type, os.name, os.version, os.description')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('host')
                                            ->info('Host detector - detects host.name, host.arch, host.id')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('service')
                                            ->info('Service detector - detects service.name and service.version from composer.json')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('deployment')
                                            ->info('Deployment detector - detects deployment.environment.name from Symfony kernel environment')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                        ->arrayNode('environment')
                                            ->info('Environment detector - reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode('dynamic')
                                    ->info('Dynamic detectors - values that may change between requests/commands (never cached)')
                                    ->addDefaultsIfNotSet()
                                    ->children()
                                        ->arrayNode('process')
                                            ->info('Process detector - detects process.pid, process.runtime.*, process.executable.*')
                                            ->addDefaultsIfNotSet()
                                            ->children()
                                                ->booleanNode('enabled')->defaultTrue()->end()
                                            ->end()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('custom')
                            ->info('Custom resource attributes that override auto-detected values')
                            ->normalizeKeys(false)
                            ->useAttributeAsKey('name')
                            ->prototype('variable')->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('clock_service_id')
                    ->info('Custom PSR-20 clock service ID. If not provided, uses built-in SystemClock.')
                    ->defaultNull()
                ->end()
                ->scalarNode('framework_logger')
                    ->info('Name of the logger (matching a key under "loggers", or "default") whose PSR-3 wrapper will be aliased to Symfony\'s "logger" service. Leave null to auto-replace only when Symfony\'s default HttpKernel Logger is currently bound.')
                    ->defaultNull()
                ->end()
                ->arrayNode('context_storage')
                    ->info('Context storage configuration')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->enumNode('type')
                            ->values(['memory', 'service'])
                            ->defaultValue('memory')
                        ->end()
                        ->scalarNode('service_id')
                            ->info('Custom context storage service ID (only for type: service)')
                            ->defaultNull()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('propagator')
                    ->info('Context propagator configuration for distributed tracing')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->enumNode('type')
                            ->info("Propagator type: 'w3c' (W3C TraceContext + Baggage), 'tracecontext' (W3C TraceContext only), 'baggage' (W3C Baggage only), 'service' (custom)")
                            ->values(['w3c', 'tracecontext', 'baggage', 'service'])
                            ->defaultValue('w3c')
                        ->end()
                        ->scalarNode('service_id')
                            ->info('Custom propagator service ID (only for type: service)')
                            ->defaultNull()
                        ->end()
                    ->end()
                ->end()
                ->append($this->errorHandlersNode())
                ->append($this->exportersNode())
                ->arrayNode('tracer_provider')
                    ->info('TracerProvider configuration. Defaults to void if omitted.')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->scalarNode('error_handler')
                            ->info('Name of an error_handler entry forwarded to the TracerProvider')
                            ->defaultValue('default')
                        ->end()
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
                        ->scalarNode('error_handler')
                            ->info('Name of an error_handler entry forwarded to the MeterProvider')
                            ->defaultValue('default')
                        ->end()
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
                        ->scalarNode('error_handler')
                            ->info('Name of an error_handler entry forwarded to the LoggerProvider')
                            ->defaultValue('default')
                        ->end()
                        ->append($this->processorNode('log'))
                    ->end()
                ->end()
                ->arrayNode('instrumentation')
                    ->info('Auto-instrumentation configuration')
                    ->addDefaultsIfNotSet()
                    ->children()
                        ->arrayNode('http_kernel')
                            ->info('HTTP kernel request tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->arrayNode('exclude_paths')
                                    ->info('URL paths to exclude from tracing (checked before route resolution)')
                                    ->arrayPrototype()
                                        ->children()
                                            ->scalarNode('path')
                                                ->info('Path pattern (exact or regex with / delimiters)')
                                                ->isRequired()
                                                ->cannotBeEmpty()
                                            ->end()
                                            ->scalarNode('method')
                                                ->info('HTTP method (omit for all methods)')
                                                ->defaultNull()
                                            ->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->booleanNode('context_propagation')
                                    ->info('Enable context propagation from incoming HTTP headers (requires propagator)')
                                    ->defaultTrue()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('console')
                            ->info('Console command tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->arrayNode('exclude_commands')
                                    ->info('Command names to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('messenger')
                            ->info('Messenger tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->booleanNode('context_propagation')
                                    ->info('Enable context propagation across message boundaries (requires propagator)')
                                    ->defaultTrue()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('twig')
                            ->info('Twig template tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->booleanNode('trace_templates')
                                    ->info('Trace template rendering')
                                    ->defaultTrue()
                                ->end()
                                ->booleanNode('trace_blocks')
                                    ->info('Trace block rendering')
                                    ->defaultFalse()
                                ->end()
                                ->booleanNode('trace_macros')
                                    ->info('Trace macro execution')
                                    ->defaultFalse()
                                ->end()
                                ->arrayNode('exclude_templates')
                                    ->info('Template paths to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('http_client')
                            ->info('HTTP client request tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->arrayNode('exclude_clients')
                                    ->info('HTTP client service IDs to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('psr18_client')
                            ->info('PSR-18 HTTP client request tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->arrayNode('exclude_clients')
                                    ->info('PSR-18 client service IDs to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('dbal')
                            ->info('Doctrine DBAL query tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->booleanNode('log_sql')
                                    ->info('Whether to include SQL in span attributes')
                                    ->defaultTrue()
                                ->end()
                                ->integerNode('max_sql_length')
                                    ->info('Maximum SQL length in span attributes (0 = no limit)')
                                    ->defaultValue(1000)
                                    ->min(0)
                                ->end()
                                ->arrayNode('exclude_connections')
                                    ->info('Connection names to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                        ->arrayNode('cache')
                            ->info('Symfony Cache tracing configuration')
                            ->canBeEnabled()
                            ->children()
                                ->arrayNode('exclude_pools')
                                    ->info('Cache pool service IDs to exclude from tracing (supports regex with / delimiters)')
                                    ->scalarPrototype()->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('tracers')
                    ->info('Named tracer configurations')
                    ->useAttributeAsKey('name')
                    ->arrayPrototype()
                        ->children()
                            ->scalarNode('version')
                                ->info('Instrumentation scope version')
                                ->defaultValue('unknown')
                            ->end()
                            ->scalarNode('schema_url')
                                ->info('Schema URL for semantic conventions')
                                ->defaultNull()
                            ->end()
                            ->arrayNode('attributes')
                                ->info('Additional scope attributes')
                                ->normalizeKeys(false)
                                ->useAttributeAsKey('name')
                                ->prototype('variable')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('meters')
                    ->info('Named meter configurations')
                    ->useAttributeAsKey('name')
                    ->arrayPrototype()
                        ->children()
                            ->scalarNode('version')
                                ->info('Instrumentation scope version')
                                ->defaultValue('unknown')
                            ->end()
                            ->scalarNode('schema_url')
                                ->info('Schema URL for semantic conventions')
                                ->defaultNull()
                            ->end()
                            ->arrayNode('attributes')
                                ->info('Additional scope attributes')
                                ->normalizeKeys(false)
                                ->useAttributeAsKey('name')
                                ->prototype('variable')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('loggers')
                    ->info('Named logger configurations')
                    ->useAttributeAsKey('name')
                    ->arrayPrototype()
                        ->children()
                            ->scalarNode('version')
                                ->info('Instrumentation scope version')
                                ->defaultValue('unknown')
                            ->end()
                            ->scalarNode('schema_url')
                                ->info('Schema URL for semantic conventions')
                                ->defaultNull()
                            ->end()
                            ->arrayNode('attributes')
                                ->info('Additional scope attributes')
                                ->normalizeKeys(false)
                                ->useAttributeAsKey('name')
                                ->prototype('variable')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();

        return $treeBuilder;
    }

    private function errorHandlersNode() : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('error_handlers');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $supportedTypes = ['error_log', 'stream', 'syslog', 'udp_syslog', 'composite', 'noop', 'service'];
        $facilities = ['auth', 'cron', 'daemon', 'kernel', 'local0', 'local1', 'local2', 'local3', 'local4', 'local5', 'local6', 'local7', 'lpr', 'mail', 'news', 'syslog', 'user', 'uucp'];
        $severities = ['alert', 'critical', 'debug', 'emergency', 'error', 'info', 'notice', 'warning'];
        $messageTypes = ['operating_system', 'email', 'file', 'sapi'];

        $node
            ->info('Named error handler definitions referenced by providers, processors, and OTLP exporters via "error_handler:" fields. If "default" is omitted it is auto-created with type: error_log.')
            ->useAttributeAsKey('name')
            ->arrayPrototype()
                ->children()
                    ->enumNode('type')
                        ->values($supportedTypes)
                        ->defaultValue('error_log')
                    ->end()
                    ->enumNode('message_type')
                        ->info('error_log message type (only for type: error_log)')
                        ->values($messageTypes)
                        ->defaultValue('operating_system')
                    ->end()
                    ->booleanNode('expand_newlines')
                        ->info('Emit one error_log() call per line (only for type: error_log)')
                        ->defaultFalse()
                    ->end()
                    ->scalarNode('message_prefix')
                        ->info('Prefix prepended to each formatted Throwable (error_log + stream)')
                        ->defaultValue('[flow-telemetry]')
                    ->end()
                    ->scalarNode('destination')
                        ->info('File path or php:// stream URI (required for type: stream)')
                        ->defaultNull()
                    ->end()
                    ->integerNode('file_permissions')
                        ->info('Permissions applied when creating new files (only for type: stream)')
                        ->defaultValue(0644)
                        ->min(0)
                        ->max(0777)
                    ->end()
                    ->booleanNode('create_directories')
                        ->info('Create parent directories of the destination if they do not exist (only for type: stream)')
                        ->defaultTrue()
                    ->end()
                    ->scalarNode('ident')
                        ->info('Syslog identity tag (syslog + udp_syslog)')
                        ->defaultValue('flow-telemetry')
                    ->end()
                    ->enumNode('facility')
                        ->info('Syslog facility (syslog + udp_syslog)')
                        ->values($facilities)
                        ->defaultValue('user')
                    ->end()
                    ->integerNode('log_opts')
                        ->info('Bitmask of LOG_* options passed to openlog() (only for type: syslog)')
                        ->defaultValue(\LOG_PID)
                    ->end()
                    ->enumNode('severity')
                        ->info('Syslog severity (syslog + udp_syslog)')
                        ->values($severities)
                        ->defaultValue('error')
                    ->end()
                    ->scalarNode('host')
                        ->info('Remote syslog host (required for type: udp_syslog)')
                        ->defaultNull()
                    ->end()
                    ->integerNode('port')
                        ->info('Remote syslog port (only for type: udp_syslog)')
                        ->defaultValue(514)
                        ->min(1)
                        ->max(65535)
                    ->end()
                    ->arrayNode('handlers')
                        ->info('Named error_handler entries fanned-out to (only for type: composite)')
                        ->scalarPrototype()->end()
                    ->end()
                    ->scalarNode('service_id')
                        ->info('Custom error handler service ID (only for type: service)')
                        ->defaultNull()
                    ->end()
                ->end()
            ->end();

        return $node;
    }

    private function exportersNode() : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('exporters');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $supportedTypes = ['otlp', 'service', 'console', 'memory', 'void'];

        $node
            ->info('Named exporter definitions referenced from per-signal processor blocks. The sub-block under each name selects the exporter implementation; "otlp" carries an embedded transport, "service" aliases an external service id.')
            ->useAttributeAsKey('name')
            ->arrayPrototype()
                ->validate()
                    ->ifTrue(static function (array $v) use ($supportedTypes) : bool {
                        $set = 0;

                        foreach ($supportedTypes as $type) {
                            if (\array_key_exists($type, $v) && $v[$type] !== null) {
                                $set++;
                            }
                        }

                        return $set !== 1;
                    })
                    ->thenInvalid('Exporter must declare exactly one of: otlp, service, console, memory, void.')
                ->end()
                ->children()
                    ->append($this->otlpExporterNode())
                    ->append($this->serviceExporterNode())
                    ->arrayNode('console')
                        ->info('Console exporter (no options)')
                        ->treatNullLike([])
                        ->canBeUnset()
                    ->end()
                    ->arrayNode('memory')
                        ->info('Memory exporter (no options)')
                        ->treatNullLike([])
                        ->canBeUnset()
                    ->end()
                    ->arrayNode('void')
                        ->info('Void/no-op exporter (no options)')
                        ->treatNullLike([])
                        ->canBeUnset()
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
                ->scalarNode('exporter')
                    ->info('Name of a top-level exporter referenced by this processor')
                    ->defaultNull()
                ->end()
                ->scalarNode('service_id')
                    ->info('Custom processor service ID (only for type: service)')
                    ->defaultNull()
                ->end()
                ->scalarNode('error_handler')
                    ->info('Name of an error_handler entry forwarded to the inner processor')
                    ->defaultValue('default')
                ->end()
            ->end();

        return $node;
    }

    private function otlpExporterNode() : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('otlp');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('OTLP exporter — embeds its transport configuration inline')
            ->canBeUnset()
            ->validate()
                ->ifTrue(static fn (array $v) : bool => !\is_array($v['transport'] ?? null) || \count($v['transport']) === 0)
                ->thenInvalid('OTLP exporter requires a "transport" configuration block.')
            ->end()
            ->children()
                ->scalarNode('error_handler')
                    ->info('Name of an error_handler entry forwarded to the OTLP exporter')
                    ->defaultValue('default')
                ->end()
                ->append($this->transportNode())
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
                ->scalarNode('exporter')
                    ->info('Name of a top-level exporter referenced by this processor')
                    ->defaultNull()
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
                ->scalarNode('error_handler')
                    ->info('Name of an error_handler entry forwarded to this processor')
                    ->defaultValue('default')
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
                            ->scalarNode('exporter')
                                ->defaultNull()
                            ->end()
                            ->scalarNode('service_id')
                                ->defaultNull()
                            ->end()
                            ->enumNode('minimum_severity')
                                ->values(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
                                ->defaultValue('info')
                            ->end()
                            ->scalarNode('error_handler')
                                ->info('Name of an error_handler entry forwarded to this child processor')
                                ->defaultValue('default')
                            ->end()
                            ->append($this->innerProcessorNode($signalType))
                        ->end()
                    ->end()
                ->end()
                ->append($this->innerProcessorNode($signalType))
            ->end();

        return $node;
    }

    private function serviceExporterNode() : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('service');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('Aliases an existing Symfony service implementing Flow\\Telemetry\\Exporter\\Exporter')
            ->canBeUnset()
            ->children()
                ->scalarNode('id')
                    ->info('Service id of the user-provided exporter (required)')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
            ->end();

        return $node;
    }

    private function transportNode() : ArrayNodeDefinition
    {
        $builder = new TreeBuilder('transport');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('Transport configuration (required when exporter type is "otlp")')
            ->beforeNormalization()
                ->always(static function (mixed $v) : mixed {
                    if (\is_array($v) && ($v['type'] ?? null) === 'grpc' && \array_key_exists('timeout', $v)) {
                        throw new InvalidConfigurationException(
                            'The "timeout" parameter is not supported when transport.type is "grpc".',
                        );
                    }

                    if (\is_array($v) && ($v['type'] ?? null) === 'stream') {
                        $forbidden = [
                            'timeout',
                            'connect_timeout',
                            'compression',
                            'follow_redirects',
                            'max_redirects',
                            'proxy',
                            'ssl_verify_peer',
                            'ssl_verify_host',
                            'ssl_cert_path',
                            'ssl_key_path',
                            'ca_info_path',
                            'headers',
                            'insecure',
                        ];

                        foreach ($forbidden as $key) {
                            if (\array_key_exists($key, $v)) {
                                throw new InvalidConfigurationException(\sprintf(
                                    'The "%s" parameter is not supported when transport.type is "stream".',
                                    $key,
                                ));
                            }
                        }
                    }

                    $encodingRejection = match (\is_array($v) ? ($v['type'] ?? null) : null) {
                        'stream' => 'only JSON encoding is allowed by the OTLP File Exporter spec',
                        'grpc' => 'OTLP/gRPC mandates Protobuf encoding',
                        default => null,
                    };

                    if ($encodingRejection !== null && \is_array($v) && \array_key_exists('encoding', $v)) {
                        throw new InvalidConfigurationException(\sprintf(
                            'The "encoding" parameter is not supported when transport.type is "%s"; %s.',
                            $v['type'],
                            $encodingRejection,
                        ));
                    }

                    return $v;
                })
            ->end()
            ->validate()
                ->ifTrue(static function (array $v) : bool {
                    if (($v['type'] ?? null) !== 'stream') {
                        return false;
                    }

                    $endpoint = $v['endpoint'] ?? null;

                    return !\is_string($endpoint) || $endpoint === '';
                })
                ->thenInvalid('The "endpoint" parameter is required and must be a non-empty string when transport.type is "stream" (used as the destination file path or php:// stream wrapper URI).')
            ->end()
            ->children()
                ->enumNode('type')
                    ->info("Transport type: 'curl', 'grpc', 'stream', 'service'")
                    ->values(['curl', 'grpc', 'stream', 'service'])
                    ->defaultValue('curl')
                ->end()
                ->scalarNode('endpoint')
                    ->info('OTLP endpoint URL for curl/grpc, or destination file path / php:// stream wrapper URI for stream (required unless type: service)')
                    ->defaultNull()
                ->end()
                ->integerNode('file_permissions')
                    ->info('Permissions applied when creating new files (stream only; ignored for php:// destinations)')
                    ->defaultValue(0644)
                    ->min(0)
                    ->max(0777)
                ->end()
                ->booleanNode('create_directories')
                    ->info('Create parent directories of the destination path if they do not exist (stream only; ignored for php:// destinations)')
                    ->defaultTrue()
                ->end()
                ->integerNode('timeout')
                    ->info('Request timeout in seconds (curl only)')
                    ->defaultValue(30)
                    ->min(1)
                ->end()
                ->arrayNode('headers')
                    ->info('Additional HTTP headers')
                    ->normalizeKeys(false)
                    ->useAttributeAsKey('name')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('connect_timeout')
                    ->info('Connection timeout in seconds (curl only)')
                    ->defaultValue(10)
                    ->min(1)
                ->end()
                ->booleanNode('compression')
                    ->info('Enable automatic response decompression (curl only)')
                    ->defaultFalse()
                ->end()
                ->booleanNode('follow_redirects')
                    ->info('Follow HTTP redirects (curl only)')
                    ->defaultTrue()
                ->end()
                ->integerNode('max_redirects')
                    ->info('Maximum number of redirects to follow (curl only)')
                    ->defaultValue(3)
                    ->min(0)
                ->end()
                ->scalarNode('proxy')
                    ->info('Proxy server URL (curl only)')
                    ->defaultNull()
                ->end()
                ->booleanNode('ssl_verify_peer')
                    ->info('Verify SSL peer certificate (curl only)')
                    ->defaultTrue()
                ->end()
                ->booleanNode('ssl_verify_host')
                    ->info('Verify SSL host name (curl only)')
                    ->defaultTrue()
                ->end()
                ->scalarNode('ssl_cert_path')
                    ->info('Path to SSL client certificate (curl only)')
                    ->defaultNull()
                ->end()
                ->scalarNode('ssl_key_path')
                    ->info('Path to SSL client private key (curl only)')
                    ->defaultNull()
                ->end()
                ->scalarNode('ca_info_path')
                    ->info('Path to CA certificate bundle (curl only)')
                    ->defaultNull()
                ->end()
                ->booleanNode('insecure')
                    ->info('Allow insecure connections (grpc only)')
                    ->defaultTrue()
                ->end()
                ->scalarNode('service_id')
                    ->info('Custom transport service ID (only for type: service)')
                    ->defaultNull()
                ->end()
                ->enumNode('encoding')
                    ->info('OTLP wire encoding (curl only); JSON or Protobuf as defined by the OTLP/HTTP spec')
                    ->values(['json', 'protobuf'])
                    ->defaultValue('json')
                ->end()
            ->end();

        return $node;
    }
}
