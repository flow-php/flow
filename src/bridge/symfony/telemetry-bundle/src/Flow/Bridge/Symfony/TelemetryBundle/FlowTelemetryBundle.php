<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle;

use Flow\Bridge\Psr3\Telemetry\LogRecordConverter;
use Flow\Bridge\Psr3\Telemetry\TelemetryLogger;
use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ArgumentResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ArgumentValueResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\CacheTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ChannelLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ControllerResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\DBALTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\MessengerTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\FrameworkLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\HttpClientTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ProfilerSignalCapturePass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\Psr18ClientTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\TraceContextUrlGeneratorPass;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\CommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleLogOutputSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\RouteNaming;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerMetricDurationUnit;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Security\UserSpanAttributeProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleOutputLogProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleVerbosityLevels;
use Flow\Bridge\Symfony\TelemetryBundle\Resource\Detector\SymfonyDeploymentDetector;
use Flow\Bridge\Symfony\TelemetryBundle\Runtime\EnvironmentWorkerModeDetector;
use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Runtime\WorkerModeDetector;
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Filter\All;
use Flow\Telemetry\Filter\Any;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeRule;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Filter\MatchMode;
use Flow\Telemetry\Filter\Not;
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
use Flow\Telemetry\Logger\Middleware\AttributeFilteringLogMiddleware;
use Flow\Telemetry\Logger\Middleware\EnrichingLogMiddleware;
use Flow\Telemetry\Logger\Middleware\SeverityFilteringLogMiddleware;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Processor\CompositeLogProcessor;
use Flow\Telemetry\Logger\Processor\PassThroughLogProcessor;
use Flow\Telemetry\Logger\Processor\PipelineLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Meter\Processor\CompositeMetricProcessor;
use Flow\Telemetry\Meter\Processor\AttributeFilteringMetricProcessor;
use Flow\Telemetry\Meter\Processor\PassThroughMetricProcessor;
use Flow\Telemetry\Propagation\CompositePropagator;
use Flow\Telemetry\Propagation\W3CBaggage;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Conditional\ConditionalExporter;
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
use Flow\Telemetry\Resource\Detector\GitDetector;
use Flow\Telemetry\Resource\Detector\HostDetector;
use Flow\Telemetry\Resource\Detector\ManualDetector;
use Flow\Telemetry\Resource\Detector\OsDetector;
use Flow\Telemetry\Resource\Detector\ProcessDetector;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\Processor\CompositeSpanProcessor;
use Flow\Telemetry\Tracer\Processor\AttributeFilteringSpanProcessor;
use Flow\Telemetry\Tracer\Processor\PassThroughSpanProcessor;
use Flow\Telemetry\Tracer\Sampler\AlwaysOffSampler;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\AttributeMatchingSampler;
use Flow\Telemetry\Tracer\Sampler\ParentBasedSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\Sampler\TraceIdRatioBasedSampler;
use Flow\Telemetry\Tracer\Tracer;
use Flow\Telemetry\Tracer\TracerProvider;
use Override;
use Psr\Clock\ClockInterface;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use Twig\Extension\AbstractExtension;

use function array_key_exists;
use function array_values;
use function class_exists;
use function count;
use function implode;
use function in_array;
use function interface_exists;
use function is_array;
use function is_bool;
use function is_int;
use function is_string;
use function sprintf;
use function sys_get_temp_dir;
use function ucfirst;

use const LOG_PID;

final class FlowTelemetryBundle extends AbstractBundle
{
    private const string CACHE_ADAPTER_INTERFACE = 'Symfony\\Component\\Cache\\Adapter\\AdapterInterface';

    private const string DBAL_MIDDLEWARE_INTERFACE = 'Doctrine\\DBAL\\Driver\\Middleware';

    private const string HTTP_CLIENT_INTERFACE = 'Symfony\\Contracts\\HttpClient\\HttpClientInterface';

    private const string HTTP_FOUNDATION_REQUEST_CARRIER = 'Flow\\Bridge\\Symfony\\HttpFoundationTelemetry\\RequestCarrier';

    private const string MESSENGER_MIDDLEWARE_INTERFACE = 'Symfony\\Component\\Messenger\\Middleware\\MiddlewareInterface';

    private const string PSR18_CLIENT_INTERFACE = 'Psr\\Http\\Client\\ClientInterface';

    private const string PSR18_TRACEABLE_CLIENT = 'Flow\\Bridge\\Psr18\\Telemetry\\PSR18TraceableClient';

    private const string SECURITY_TOKEN_STORAGE_INTERFACE = 'Symfony\\Component\\Security\\Core\\Authentication\\Token\\Storage\\TokenStorageInterface';

    private const string URL_GENERATOR_INTERFACE = 'Symfony\\Component\\Routing\\Generator\\UrlGeneratorInterface';

    private const string WEB_PROFILER_BUNDLE = 'Symfony\\Bundle\\WebProfilerBundle\\WebProfilerBundle';

    #[Override]
    public function getPath(): string
    {
        return __DIR__;
    }

    #[Override]
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);

        $container->addCompilerPass(new OTLPAvailabilityPass());
        $container->addCompilerPass(new ProfilerSignalCapturePass());
        $container->addCompilerPass(new ControllerResolverTelemetryPass());
        $container->addCompilerPass(new ArgumentResolverTelemetryPass());
        $container->addCompilerPass(new ArgumentValueResolverTelemetryPass());
        $container->addCompilerPass(new FrameworkLoggerPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -64);

        $container->registerAttributeForAutoconfiguration(
            WithTelemetryChannel::class,
            static function (ChildDefinition $definition, WithTelemetryChannel $attribute): void {
                $definition->addTag(ChannelLoggerPass::TAG, ['channel' => $attribute->channel]);
            },
        );
        $container->addCompilerPass(new ChannelLoggerPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION);

        $container->registerForAutoconfiguration(UserSpanAttributeProvider::class)
            ->addTag('flow.telemetry.security.user_attribute_provider');

        $container->addCompilerPass(new TraceContextUrlGeneratorPass());

        if (interface_exists(self::HTTP_CLIENT_INTERFACE)) {
            $container->addCompilerPass(new HttpClientTelemetryPass());
        }

        if (interface_exists(self::PSR18_CLIENT_INTERFACE) && class_exists(self::PSR18_TRACEABLE_CLIENT)) {
            $container->addCompilerPass(new Psr18ClientTelemetryPass());
        }

        if (interface_exists(self::DBAL_MIDDLEWARE_INTERFACE)) {
            $container->addCompilerPass(
                new DBALTelemetryPass(),
                PassConfig::TYPE_BEFORE_OPTIMIZATION,
                1,
            );
        }

        if (interface_exists(self::CACHE_ADAPTER_INTERFACE)) {
            $container->addCompilerPass(new CacheTelemetryPass());
        }

        if (interface_exists(self::MESSENGER_MIDDLEWARE_INTERFACE)) {
            // Must run before Symfony's MessengerPass (BEFORE_OPTIMIZATION, priority 0), which reads the
            // "<busId>.middleware" parameter this pass prepends to.
            $container->addCompilerPass(new MessengerTelemetryPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, 32);
        }
    }

    #[Override]
    public function configure(DefinitionConfigurator $definition): void
    {
        $definition
            ->rootNode()
            ->children()
            ->arrayNode('resource')
            ->info(
                'OpenTelemetry Resource configuration with automatic detection (https://opentelemetry.io/docs/specs/semconv/resource/)',
            )
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
            ->info(
                'File-based caching of static resource attributes. The cache file is intentionally outside Symfony\'s cache lifecycle so build-time cache:warmup does not freeze runtime-dependent attributes (host, process).',
            )
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->info('Enable caching of static resource attributes')
            ->defaultTrue()
            ->end()
            ->scalarNode('path')
            ->info('Absolute path to the cache file. Default: sys_get_temp_dir()/flow_telemetry_resource_<kernel.environment>.cache (keyed by kernel environment so switching APP_ENV does not serve a stale deployment.environment.name).')
            ->defaultNull()
            ->end()
            ->end()
            ->end()
            ->arrayNode('os')
            ->info('OS detector - detects os.type, os.name, os.version, os.description')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
            ->end()
            ->end()
            ->arrayNode('host')
            ->info('Host detector - detects host.name, host.arch, host.id')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
            ->end()
            ->end()
            ->arrayNode('service')
            ->info('Service detector - detects service.name and service.version from composer.json')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
            ->end()
            ->end()
            ->arrayNode('deployment')
            ->info('Deployment detector - detects deployment.environment.name from Symfony kernel environment')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
            ->end()
            ->end()
            ->arrayNode('git')
            ->info(
                'Git detector - detects vcs.ref.head.* and vcs.repository.url.full by invoking the git binary. Disabled by default; the remote URL is reported with any embedded credentials stripped.',
            )
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->info('Enable the git detector (default: false)')
            ->defaultFalse()
            ->end()
            ->scalarNode('binary')
            ->info('Path to the git binary (default: "git", resolved from $PATH)')
            ->defaultValue('git')
            ->end()
            ->scalarNode('working_directory')
            ->info('Directory to run git in (default: %kernel.project_dir%)')
            ->defaultNull()
            ->end()
            ->end()
            ->end()
            ->arrayNode('environment')
            ->info('Environment detector - reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
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
            ->booleanNode('enabled')
            ->defaultTrue()
            ->end()
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
            ->prototype('variable')
            ->end()
            ->end()
            ->end()
            ->end()
            ->scalarNode('clock_service_id')
            ->info('Custom PSR-20 clock service ID. If not provided, uses built-in SystemClock.')
            ->defaultNull()
            ->end()
            ->scalarNode('framework_logger')
            ->info(
                'Name of the logger (matching a key under "loggers", or "default") whose PSR-3 wrapper will be aliased to Symfony\'s "logger" service. Leave null to auto-replace only when Symfony\'s default HttpKernel Logger is currently bound.',
            )
            ->defaultNull()
            ->end()
            ->booleanNode('capture_framework_channels')
            ->info(
                'Reroute services Symfony tags "monolog.logger" to per-channel Flow telemetry loggers. Disabled by default to avoid colliding with MonologBundle, which claims the same tag.',
            )
            ->defaultFalse()
            ->end()
            ->enumNode('channel_attribute_target')
            ->info(
                'Where the "log.channel" attribute of a synthesized channel logger is placed: "scope" (instrumentation scope), "signal" (every emitted record), or "both" (default). Applies to framework-captured and #[WithTelemetryChannel] channels alike.',
            )
            ->values(['scope', 'signal', 'both'])
            ->defaultValue('both')
            ->end()
            ->enumNode('runtime_mode')
            ->info(
                'How telemetry is drained at request/command boundaries: "classic" (PHP-FPM, one process per request — shutdown on terminate), "worker" (long-running runtime — flush on terminate, never shutdown), or "auto" (detect FrankenPHP/RoadRunner worker mode at runtime, falling back to classic). Default: auto.',
            )
            ->values(['auto', 'classic', 'worker'])
            ->defaultValue('auto')
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
            ->info(
                "Propagator type: 'w3c' (W3C TraceContext + Baggage), 'tracecontext' (W3C TraceContext only), 'baggage' (W3C Baggage only), 'service' (custom)",
            )
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
            ->values(['always_on', 'always_off', 'trace_id_ratio', 'parent_based', 'attribute_matching', 'service'])
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
            ->booleanNode('exclude')
            ->info('attribute_matching: when true (default) a match drops the span; when false only matching spans are sampled')
            ->defaultTrue()
            ->end()
            ->arrayNode('sources')
            ->info('attribute_matching: which attribute sets to inspect - any of signal (default), resource, scope')
            ->enumPrototype()
            ->values(['signal', 'resource', 'scope'])
            ->end()
            ->defaultValue(['signal'])
            ->end()
            ->scalarNode('cache_dir')
            ->info('attribute_matching: directory for the generated matcher cache. Defaults to %kernel.cache_dir%/flow_telemetry_filters')
            ->defaultNull()
            ->end()
            ->integerNode('cache_dir_permissions')
            ->info('attribute_matching: octal mode applied when the matcher cache directory is created (default 0o700, owner-only; write it as a YAML octal literal e.g. 0o750)')
            ->defaultValue(0o700)
            ->min(0)
            ->max(0o777)
            ->end()
            ->variableNode('matcher')
            ->info('attribute_matching: the matcher tree (all/any/not or a leaf rule { path, mode, value, case_sensitive? }); nestable to any depth')
            ->defaultNull()
            ->end()
            ->arrayNode('delegate')
            ->info('attribute_matching: sampler that decides spans which do not match (default: always_on)')
            ->addDefaultsIfNotSet()
            ->children()
            ->enumNode('type')
            ->values(['always_on', 'always_off', 'trace_id_ratio'])
            ->defaultValue('always_on')
            ->end()
            ->floatNode('ratio')
            ->defaultValue(1.0)
            ->min(0.0)
            ->max(1.0)
            ->end()
            ->end()
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
            ->arrayNode('console_output')
            ->info(
                'Tee emitted log records to the running console command output, filtered by CLI verbosity (-v/-vv/-vvv). The Flow equivalent of Symfony\'s Monolog ConsoleHandler; display only, exporters are unaffected. Off by default.',
            )
            ->canBeEnabled()
            ->children()
            ->arrayNode('verbosity_levels')
            ->info(
                'Override the verbosity->minimum-severity thresholds. Keys: VERBOSITY_QUIET, VERBOSITY_NORMAL, VERBOSITY_VERBOSE, VERBOSITY_VERY_VERBOSE, VERBOSITY_DEBUG. Values: TRACE, DEBUG, INFO, WARN, ERROR, FATAL. Defaults: QUIET=ERROR, NORMAL=WARN, VERBOSE=INFO, VERY_VERBOSE=DEBUG, DEBUG=TRACE.',
            )
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->enumPrototype()
            ->values(['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])
            ->end()
            ->end()
            ->end()
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
            ->info('Extract trace context from incoming request headers and inject it into outgoing response headers (requires flow-php/symfony-http-foundation-telemetry-bridge; silently disabled when absent)')
            ->defaultTrue()
            ->end()
            ->booleanNode('context_propagation_query')
            ->info('Also extract trace context from the URL query string (for links / full-page navigations that cannot send headers); headers take precedence. Security: lets callers inject a traceparent, so keep off unless needed. Requires context_propagation.')
            ->defaultFalse()
            ->end()
            ->enumNode('route_naming')
            ->info('What routed request spans use for their name and the http.route attribute: "path" (route path template, e.g. /orders/{id}; OTEL semconv default) or "name" (Symfony route name). Sub-requests fall back to the controller; unrouted requests use the method only.')
            ->values(['path', 'name'])
            ->defaultValue('path')
            ->end()
            ->booleanNode('trace_controller')
            ->info('Trace controller body execution as a child of the request span (span name = resolved controller)')
            ->defaultTrue()
            ->end()
            ->booleanNode('trace_controller_resolution')
            ->info('Trace controller resolution (controller.get_callable span); opt-in, finer detail')
            ->defaultFalse()
            ->end()
            ->booleanNode('trace_controller_arguments')
            ->info('Trace argument resolution as a single aggregate span (controller.get_arguments); opt-in')
            ->defaultFalse()
            ->end()
            ->booleanNode('trace_controller_argument_resolvers')
            ->info('Trace each argument value resolver individually (controller.argument_value_resolver span); opt-in, higher cardinality')
            ->defaultFalse()
            ->end()
            ->end()
            ->end()
            ->arrayNode('console')
            ->info('Console command tracing configuration')
            ->canBeEnabled()
            ->children()
            ->arrayNode('exclude_commands')
            ->info('Command names whose tracing is fully suppressed: the command and every span nested under '
                . 'it are dropped (supports regex with / delimiters). Instrumentation that starts its own '
                . 'root trace — e.g. messenger per-message handlers — is unaffected. Honored regardless of '
                . 'whether console spans are enabled. Set to [] to trace everything (including messenger:consume).')
            ->scalarPrototype()
            ->end()
            ->defaultValue(['messenger:consume'])
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
            ->booleanNode('trace')
            ->info('Emit a per-message span for each consumed/produced message. When false, the worker is fully '
                . 'suppressed and only metrics are emitted (if metrics are enabled).')
            ->defaultTrue()
            ->end()
            ->booleanNode('metrics')
            ->info('Emit OTEL messaging metrics for consumed/sent messages (messaging.client.consumed.messages, messaging.client.sent.messages) and processing duration (messaging.process.duration)')
            ->defaultTrue()
            ->end()
            ->enumNode('metrics_duration_unit')
            ->info('Unit for the messaging.process.duration histogram: "s" (OTEL semconv default) or "ms" (Flow-native histogram buckets)')
            ->values(['s', 'ms'])
            ->defaultValue('s')
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
            ->scalarPrototype()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('security')
            ->info('Decorate the request span with the authenticated user (requires symfony/security-core; login capture requires symfony/security-http)')
            ->canBeEnabled()
            ->children()
            ->arrayNode('fields')
            ->addDefaultsIfNotSet()
            ->children()
            ->arrayNode('id')
            ->info('User identifier (TokenInterface::getUserIdentifier())')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')->defaultTrue()->end()
            ->scalarNode('attribute')->info('Span attribute key')->defaultValue('user.id')->cannotBeEmpty()->end()
            ->end()
            ->end()
            ->arrayNode('roles')
            ->info('Token role names (TokenInterface::getRoleNames())')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')->defaultFalse()->end()
            ->scalarNode('attribute')->info('Span attribute key')->defaultValue('user.roles')->cannotBeEmpty()->end()
            ->end()
            ->end()
            ->arrayNode('email')
            ->info('User email, read from a getter on the user object')
            ->addDefaultsIfNotSet()
            ->children()
            ->booleanNode('enabled')->defaultFalse()->end()
            ->scalarNode('attribute')->info('Span attribute key')->defaultValue('user.email')->cannotBeEmpty()->end()
            ->scalarNode('getter')->info('User method to read the email from')->defaultValue('getEmail')->cannotBeEmpty()->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('http_client')
            ->info('HTTP client request tracing configuration')
            ->canBeEnabled()
            ->children()
            ->arrayNode('exclude_clients')
            ->info('HTTP client service IDs to exclude from tracing (supports regex with / delimiters)')
            ->scalarPrototype()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('psr18_client')
            ->info('PSR-18 HTTP client request tracing configuration')
            ->canBeEnabled()
            ->children()
            ->arrayNode('exclude_clients')
            ->info('PSR-18 client service IDs to exclude from tracing (supports regex with / delimiters)')
            ->scalarPrototype()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('dbal')
            ->info('Doctrine DBAL query tracing configuration')
            ->canBeEnabled()
            ->children()
            ->integerNode('max_sql_length')
            ->info('Maximum SQL length in span attributes (0 = no limit)')
            ->defaultValue(1000)
            ->min(0)
            ->end()
            ->booleanNode('collect_metrics')
            ->info('Record db.client.operation.duration and db.client.response.returned_rows histograms')
            ->defaultTrue()
            ->end()
            ->booleanNode('include_parameters')
            ->info('Include bound statement parameters in span attributes (security consideration)')
            ->defaultFalse()
            ->end()
            ->integerNode('max_parameters')
            ->info('Maximum number of parameters to include when include_parameters is enabled')
            ->defaultValue(10)
            ->min(0)
            ->end()
            ->integerNode('max_parameter_length')
            ->info('Maximum length for each included parameter value')
            ->defaultValue(100)
            ->min(0)
            ->end()
            ->enumNode('transaction_spans')
            ->info('How transactions are traced: "grouped" (one BEGIN TRANSACTION span holding the queries), "per_operation" (a short span per BEGIN/COMMIT/ROLLBACK), or "off" (no transaction spans)')
            ->values(['grouped', 'per_operation', 'off'])
            ->defaultValue('grouped')
            ->end()
            ->arrayNode('exclude_connections')
            ->info('Connection names to exclude from tracing (supports regex with / delimiters)')
            ->scalarPrototype()
            ->end()
            ->end()
            ->arrayNode('exclude_tables')
            ->info('Table names whose queries are not traced (e.g. "cache_items" behind a Doctrine DBAL cache pool); matched case-insensitively on whole words in the SQL')
            ->scalarPrototype()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('cache')
            ->info('Symfony Cache tracing configuration')
            ->canBeEnabled()
            ->children()
            ->arrayNode('exclude_pools')
            ->info('Cache pool service IDs to exclude from tracing (supports regex with / delimiters)')
            ->scalarPrototype()
            ->end()
            ->end()
            ->booleanNode('flush_deferred')
            ->info('Commit deferred cache writes on request/command termination and after each consumed message, inside one "cache.flush" span, so a Doctrine DBAL pool\'s deferred writes group under a single trace instead of orphaning at process shutdown')
            ->defaultFalse()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('profiler')
            ->info('Symfony Web Profiler integration (dev only; requires symfony/web-profiler-bundle)')
            ->canBeUnset()
            ->addDefaultsIfNotSet()
            ->children()
            ->variableNode('enabled')
            ->info(
                'null (default) = auto-enable iff WebProfilerBundle is present; true/false to force on/off',
            )
            ->defaultNull()
            ->validate()
            ->ifTrue(static fn (mixed $v): bool => $v !== null && !is_bool($v))
            ->thenInvalid('flow_telemetry.profiler.enabled must be true, false, or null')
            ->end()
            ->end()
            ->booleanNode('capture_logs')
            ->info('Also tee logs into the profiler store (Symfony already has a Logs panel; off by default)')
            ->defaultFalse()
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
            ->info('Scope and signal attribute defaults for this instrumentation scope')
            ->children()
            ->arrayNode('scope')
            ->info('Attributes attached to the instrumentation scope')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->arrayNode('signal')
            ->info('Default attributes merged into every emitted signal (per-call values win)')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->end()
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
            ->info('Scope and signal attribute defaults for this instrumentation scope')
            ->children()
            ->arrayNode('scope')
            ->info('Attributes attached to the instrumentation scope')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->arrayNode('signal')
            ->info('Default attributes merged into every emitted signal (per-call values win)')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->end()
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
            ->info('Scope and signal attribute defaults for this instrumentation scope')
            ->children()
            ->arrayNode('scope')
            ->info('Attributes attached to the instrumentation scope')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->arrayNode('signal')
            ->info('Default attributes merged into every emitted signal (per-call values win)')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end();
    }

    /**
     * @param array{resource: array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, git?: array{enabled?: bool, binary?: string, working_directory?: null|string}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>}, clock_service_id?: null|string, framework_logger?: null|string, capture_framework_channels?: bool, channel_attribute_target?: 'scope'|'signal'|'both', runtime_mode?: 'auto'|'classic'|'worker', context_storage?: array{type?: string, service_id?: null|string}, propagator?: array{type?: string, service_id?: null|string}, exporters?: array<string, array<string, mixed>>, error_handlers?: array<string, array<string, mixed>>, tracer_provider?: array<string, mixed>, meter_provider?: array<string, mixed>, logger_provider?: array<string, mixed>, instrumentation?: array{http_kernel?: array{enabled?: bool, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool, trace_controller?: bool, trace_controller_resolution?: bool, trace_controller_arguments?: bool, trace_controller_argument_resolvers?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool, trace?: bool, metrics?: bool, metrics_duration_unit?: 's'|'ms'}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, max_sql_length?: int, collect_metrics?: bool, include_parameters?: bool, max_parameters?: int, max_parameter_length?: int, transaction_spans?: 'grouped'|'per_operation'|'off', exclude_connections?: array<string>, exclude_tables?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>, flush_deferred?: bool}}, profiler?: array{enabled?: bool|null, capture_logs?: bool}, tracers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}}>, meters?: array<string, array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}}>, loggers?: array<string, array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}}>} $config
     */
    #[Override]
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $builder->setParameter('flow.telemetry.framework_logger', $config['framework_logger'] ?? null);
        $builder->setParameter('flow.telemetry.capture_framework_channels', $config['capture_framework_channels'] ?? false);
        $builder->setParameter('flow.telemetry.channel_attribute_target', $config['channel_attribute_target'] ?? 'both');

        $tracers = ($config['tracers'] ?? []) + ['default' => []];
        $meters = ($config['meters'] ?? []) + ['default' => []];
        $loggers = ($config['loggers'] ?? []) + ['default' => []];

        $this->registerGlobalServices($config, $builder);
        $errorHandlers = $config['error_handlers'] ?? [];
        $this->registerErrorHandlers($errorHandlers, $builder);
        $propagator = is_array($config['propagator'] ?? null) ? $config['propagator'] : [];
        $this->registerPropagator($propagator, $builder);
        $resource = $config['resource'];
        $this->registerResource($resource, $builder);
        $exporters = is_array($config['exporters'] ?? null) ? $config['exporters'] : [];
        $this->registerNamedExporters($exporters, $builder);
        $this->registerTelemetry($config, $builder);
        $instrumentation = is_array($config['instrumentation'] ?? null) ? $config['instrumentation'] : [];
        $this->registerInstrumentation($instrumentation, $container, $builder);
        $this->registerProfiler($config, $container, $builder);
        $this->registerTracers($tracers, $builder);
        $this->registerMeters($meters, $builder);
        $this->registerLoggers($loggers, $builder);
    }

    private function applyTransportSchema(ArrayNodeDefinition $node, bool $allowFailover): void
    {
        $node
            ->beforeNormalization()
            ->always(static function (mixed $v) use ($allowFailover): mixed {
                if (!is_array($v)) {
                    return $v;
                }

                // Per-type timeout defaults that differ from the shared schema node defaults.
                // The synchronous curl transport blocks for the whole request, so it gets a generous
                // request budget; the async transport idles between pumps, so it gets a generous connect
                // budget. Injecting here (before node defaults merge) keeps grpc/stream on the shared defaults.
                $resolvedType = $v['type'] ?? 'curl';

                if ($resolvedType === 'curl' && !array_key_exists('timeout_ms', $v)) {
                    $v['timeout_ms'] = CurlTransportOptions::DEFAULT_TIMEOUT_MS;
                }

                if ($resolvedType === 'async_curl' && !array_key_exists('connect_timeout_ms', $v)) {
                    $v['connect_timeout_ms'] = AsyncCurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS;
                }

                if (($v['type'] ?? null) === 'grpc' && array_key_exists('connect_timeout_ms', $v)) {
                    throw new InvalidConfigurationException(
                        'The "connect_timeout_ms" parameter is not supported when transport.type is "grpc"; gRPC uses the per-call deadline (timeout_ms) for connection establishment too.',
                    );
                }

                if (($v['type'] ?? 'curl') !== 'async_curl' && array_key_exists('pump_timeout_ms', $v)) {
                    throw new InvalidConfigurationException(
                        'The "pump_timeout_ms" parameter is only supported when transport.type is "async_curl"; it bounds the per-tick() cooperative pump of the curl_multi transport.',
                    );
                }

                if (($v['type'] ?? null) === 'stream') {
                    $forbidden = [
                        'timeout_ms',
                        'connect_timeout_ms',
                        'shutdown_timeout_ms',
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
                        if (array_key_exists($key, $v)) {
                            throw new InvalidConfigurationException(sprintf(
                                'The "%s" parameter is not supported when transport.type is "stream".',
                                $key,
                            ));
                        }
                    }
                }

                $encodingRejection = match ($v['type'] ?? null) {
                    'stream' => 'only JSON encoding is allowed by the OTLP File Exporter spec',
                    'grpc' => 'OTLP/gRPC mandates Protobuf encoding',
                    default => null,
                };

                if ($encodingRejection !== null && array_key_exists('encoding', $v)) {
                    throw new InvalidConfigurationException(sprintf(
                        'The "encoding" parameter is not supported when transport.type is "%s"; %s.',
                        $v['type'],
                        $encodingRejection,
                    ));
                }

                if (
                    $allowFailover
                    && array_key_exists('failover', $v)
                    && is_array($v['failover'])
                    && $v['failover'] !== []
                ) {
                    $primaryType = $v['type'] ?? 'curl';

                    if (!in_array($primaryType, ['curl', 'async_curl', 'grpc'], true)) {
                        throw new InvalidConfigurationException(sprintf(
                            'The "failover" block is only supported for transport.type "curl", "async_curl" or "grpc"; got "%s".',
                            $primaryType,
                        ));
                    }
                }

                return $v;
            })
            ->end()
            ->validate()
            ->ifTrue(static function (array $v): bool {
                if (($v['type'] ?? null) !== 'stream') {
                    return false;
                }

                $endpoint = $v['endpoint'] ?? null;

                return !is_string($endpoint) || $endpoint === '';
            })
            ->thenInvalid(
                'The "endpoint" parameter is required and must be a non-empty string when transport.type is "stream" (used as the destination file path or php:// stream wrapper URI).',
            )
            ->end();

        $children = $node->children();

        $children
            ->enumNode('type')
            ->info("Transport type: 'curl' (synchronous), 'async_curl' (curl_multi, requires pumping), 'grpc', 'stream', 'service'")
            ->values(['curl', 'async_curl', 'grpc', 'stream', 'service'])
            ->defaultValue('curl')
            ->end()
            ->scalarNode('endpoint')
            ->info(
                'OTLP endpoint URL for curl/grpc, or destination file path / php:// stream wrapper URI for stream (required unless type: service)',
            )
            ->defaultNull()
            ->end()
            ->integerNode('file_permissions')
            ->info('Permissions applied when creating new files (stream only; ignored for php:// destinations)')
            ->defaultValue(0644)
            ->min(0)
            ->max(0777)
            ->end()
            ->booleanNode('create_directories')
            ->info(
                'Create parent directories of the destination path if they do not exist (stream only; ignored for php:// destinations)',
            )
            ->defaultTrue()
            ->end()
            ->integerNode('timeout_ms')
            ->info('Per-request deadline in milliseconds (curl: total request, default 10000ms; async_curl/grpc: default 5000ms).')
            ->defaultValue(5000)
            ->min(1)
            ->end()
            ->arrayNode('headers')
            ->info('Additional HTTP headers')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('scalar')
            ->end()
            ->end()
            ->integerNode('connect_timeout_ms')
            ->info('Connection-establishment deadline in milliseconds (curl/async_curl only; curl default 250ms, async_curl default 1500ms).')
            ->defaultValue(250)
            ->min(1)
            ->end()
            ->integerNode('pump_timeout_ms')
            ->info('Per-tick() bounded pump budget in milliseconds (async_curl only). 0 = single non-blocking exec round. Default 100ms.')
            ->defaultValue(100)
            ->min(0)
            ->end()
            ->integerNode('shutdown_timeout_ms')
            ->info(
                'Wall-clock budget in milliseconds for draining pending requests at shutdown (curl/grpc). Default 5000ms.',
            )
            ->defaultValue(5000)
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
            ->end();

        if ($allowFailover) {
            $children->append($this->transportNode('failover', allowFailover: false));
        }

        $children->end();
    }

    /**
     * @param array<array-key, mixed> $transportConfig
     */
    private function buildEmbeddedOtlpTransport(
        string $exporterName,
        array $transportConfig,
        ContainerBuilder $builder,
        bool $allowFailover = true,
        ?Reference $errorHandlerRef = null,
    ): string {
        $transportServiceId = 'flow.telemetry.exporter.' . $exporterName . '.transport';
        // @mago-expect analysis:mixed-assignment
        $type = $transportConfig['type'] ?? 'curl';

        if ($type === 'service') {
            // @mago-expect analysis:mixed-assignment
            $customServiceId = $transportConfig['service_id'] ?? null;

            if (!is_string($customServiceId) || $customServiceId === '') {
                throw new RuntimeException(sprintf(
                    'service_id is required when exporter "%s" transport type is "service"',
                    $exporterName,
                ));
            }
            $builder->setAlias($transportServiceId, $customServiceId);

            return $transportServiceId;
        }

        // @mago-expect analysis:mixed-assignment
        $endpoint = $transportConfig['endpoint'] ?? null;

        if (!is_string($endpoint) || $endpoint === '') {
            throw new RuntimeException(sprintf('exporter "%s" transport requires an endpoint', $exporterName));
        }

        if ($type === 'stream') {
            $definition = new Definition(StreamTransport::class);
            $definition->setArgument(0, $endpoint);
            $definition->setArgument(1, $transportConfig['file_permissions'] ?? 0644);
            $definition->setArgument(2, $transportConfig['create_directories'] ?? true);
            $builder->setDefinition($transportServiceId, $definition);

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

                // @mago-expect analysis:mixed-assignment
                $headers = $transportConfig['headers'] ?? [];

                // @mago-expect analysis:mixed-assignment
                foreach (is_array($headers) ? $headers : [] as $headerName => $headerValue) {
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

                $builder->setDefinition($optionsServiceId, $optionsDefinition);

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
                    $builder,
                    $allowFailover,
                    $errorHandlerRef,
                );

                if ($failoverReference !== null) {
                    $definition->setArgument(3, $failoverReference);
                }

                $builder->setDefinition($transportServiceId, $definition);

                break;

            case 'async_curl':
                $optionsServiceId = $transportServiceId . '.options';
                $optionsDefinition = new Definition(AsyncCurlTransportOptions::class);
                $optionsDefinition->addMethodCall('withTimeout', [
                    $transportConfig['timeout_ms'] ?? AsyncCurlTransportOptions::DEFAULT_TIMEOUT_MS,
                ]);
                $optionsDefinition->addMethodCall('withConnectTimeout', [
                    $transportConfig['connect_timeout_ms'] ?? AsyncCurlTransportOptions::DEFAULT_CONNECT_TIMEOUT_MS,
                ]);
                $optionsDefinition->addMethodCall('withPumpTimeout', [
                    $transportConfig['pump_timeout_ms'] ?? AsyncCurlTransportOptions::DEFAULT_PUMP_TIMEOUT_MS,
                ]);
                $optionsDefinition->addMethodCall('withShutdownTimeout', [
                    $transportConfig['shutdown_timeout_ms'] ?? AsyncCurlTransportOptions::DEFAULT_SHUTDOWN_TIMEOUT_MS,
                ]);

                // @mago-expect analysis:mixed-assignment
                $headers = $transportConfig['headers'] ?? [];

                // @mago-expect analysis:mixed-assignment
                foreach (is_array($headers) ? $headers : [] as $headerName => $headerValue) {
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

                $builder->setDefinition($optionsServiceId, $optionsDefinition);

                $serializerClass = match ($transportConfig['encoding'] ?? 'json') {
                    'protobuf' => ProtobufSerializer::class,
                    default => JsonSerializer::class,
                };

                $definition = new Definition(AsyncCurlTransport::class);
                $definition->setArgument(0, $endpoint);
                $definition->setArgument(1, new Definition($serializerClass));
                $definition->setArgument(2, new Reference($optionsServiceId));

                $failoverReference = $this->buildFailoverTransport(
                    $exporterName,
                    $transportConfig,
                    $builder,
                    $allowFailover,
                    $errorHandlerRef,
                );

                if ($failoverReference !== null) {
                    $definition->setArgument(3, $failoverReference);
                }

                if ($errorHandlerRef !== null) {
                    $definition->setArgument('$errorHandler', $errorHandlerRef);
                }

                $definition->addTag('flow.telemetry.async_curl_transport');

                $builder->setDefinition($transportServiceId, $definition);

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
                    $builder,
                    $allowFailover,
                );

                if ($failoverReference !== null) {
                    $definition->setArgument(5, $failoverReference);
                }

                $builder->setDefinition($transportServiceId, $definition);

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
     * @param array<array-key, mixed> $handlerConfig
     */
    private function buildErrorHandlerDefinition(string $name, array $handlerConfig, ContainerBuilder $builder): void
    {
        $serviceId = 'flow.telemetry.error_handler.' . $name;
        // @mago-expect analysis:mixed-assignment
        $type = $handlerConfig['type'] ?? 'error_log';

        switch ($type) {
            case 'error_log':
                $definition = new Definition(ErrorLogHandler::class);
                // @mago-expect analysis:mixed-assignment
                $messageType = $handlerConfig['message_type'] ?? 'operating_system';
                $definition->setArgument(
                    0,
                    $this->mapErrorLogMessageType(is_string($messageType) ? $messageType : 'operating_system'),
                );
                $definition->setArgument(1, $handlerConfig['expand_newlines'] ?? false);
                $definition->setArgument(2, $handlerConfig['message_prefix'] ?? '[flow-telemetry]');
                $builder->setDefinition($serviceId, $definition);

                break;

            case 'stream':
                // @mago-expect analysis:mixed-assignment
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
                $builder->setDefinition($serviceId, $definition);

                break;

            case 'syslog':
                // @mago-expect analysis:mixed-assignment
                $syslogFacility = $handlerConfig['facility'] ?? 'user';
                // @mago-expect analysis:mixed-assignment
                $syslogSeverity = $handlerConfig['severity'] ?? 'error';
                $definition = new Definition(SyslogHandler::class);
                $definition->setArgument(0, $handlerConfig['ident'] ?? 'flow-telemetry');
                $definition->setArgument(1, $this->mapSyslogFacility(is_string($syslogFacility) ? $syslogFacility : 'user'));
                $definition->setArgument(2, $handlerConfig['log_opts'] ?? LOG_PID);
                $definition->setArgument(3, $this->mapSyslogSeverity(is_string($syslogSeverity) ? $syslogSeverity : 'error'));
                $builder->setDefinition($serviceId, $definition);

                break;

            case 'udp_syslog':
                // @mago-expect analysis:mixed-assignment
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
                // @mago-expect analysis:mixed-assignment
                $udpFacility = $handlerConfig['facility'] ?? 'user';
                // @mago-expect analysis:mixed-assignment
                $udpSeverity = $handlerConfig['severity'] ?? 'error';
                $definition->setArgument(3, $this->mapSyslogFacility(is_string($udpFacility) ? $udpFacility : 'user'));
                $definition->setArgument(4, $this->mapSyslogSeverity(is_string($udpSeverity) ? $udpSeverity : 'error'));
                $builder->setDefinition($serviceId, $definition);

                break;

            case 'composite':
                // @mago-expect analysis:mixed-assignment
                $children = $handlerConfig['handlers'] ?? [];

                if (!is_array($children) || count($children) === 0) {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "composite" requires a non-empty "handlers" list',
                        $name,
                    ));
                }
                $childRefs = [];

                // @mago-expect analysis:mixed-assignment
                foreach ($children as $childName) {
                    $childRefs[] = $this->resolveErrorHandlerReference($childName, $builder);
                }
                $builder->setDefinition($serviceId, new Definition(CompositeErrorHandler::class, $childRefs));

                break;

            case 'noop':
                $builder->setDefinition($serviceId, new Definition(NullErrorHandler::class));

                break;

            case 'service':
                // @mago-expect analysis:mixed-assignment
                $customServiceId = $handlerConfig['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException(sprintf(
                        'error_handler "%s" of type "service" requires a non-empty "service_id"',
                        $name,
                    ));
                }
                $builder->setAlias($serviceId, $customServiceId);

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
     * @param array<array-key, mixed> $transportConfig
     */
    private function buildFailoverTransport(
        string $exporterName,
        array $transportConfig,
        ContainerBuilder $builder,
        bool $allowFailover,
        ?Reference $errorHandlerRef = null,
    ): ?Reference {
        if (!$allowFailover) {
            return null;
        }

        // @mago-expect analysis:mixed-assignment
        $failoverConfig = $transportConfig['failover'] ?? null;

        if (!is_array($failoverConfig) || $failoverConfig === []) {
            return null;
        }

        $failoverServiceId = $this->buildEmbeddedOtlpTransport(
            $exporterName . '.failover',
            $failoverConfig,
            $builder,
            allowFailover: false,
            errorHandlerRef: $errorHandlerRef,
        );

        return new Reference($failoverServiceId);
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildLoggerProvider(array $config, ContainerBuilder $builder): string
    {
        $providerServiceId = 'flow.telemetry.logger_provider';

        $processorConfig = is_array($config['processor'] ?? null) ? $config['processor'] : [];
        $processorServiceId = $this->buildLogProcessor($processorConfig, $providerServiceId, $builder);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        $consoleOutputConfig = is_array($config['console_output'] ?? null) ? $config['console_output'] : [];

        if ((bool) ($consoleOutputConfig['enabled'] ?? false)) {
            $processorServiceId = $this->buildConsoleOutputLogging(
                $consoleOutputConfig,
                $providerServiceId,
                $processorServiceId,
                $errorHandlerRef,
                $builder,
            );
        }

        $definition = new Definition(LoggerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $builder->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * Wraps the configured export processor in a composite that also tees records to
     * the running console command output, and registers the supporting holder and
     * event subscriber. Returns the id of the composite to use as the provider's processor.
     *
     * @param array<array-key, mixed> $config
     */
    private function buildConsoleOutputLogging(
        array $config,
        string $providerServiceId,
        string $exportProcessorServiceId,
        Reference $errorHandlerRef,
        ContainerBuilder $builder,
    ): string {
        /** @var array<string, string> $verbosityLevels */
        $verbosityLevels = is_array($config['verbosity_levels'] ?? null) ? $config['verbosity_levels'] : [];
        $levelsDefinition = new Definition(
            ConsoleVerbosityLevels::class,
            [ConsoleVerbosityLevels::fromOverrides($verbosityLevels)->thresholds()],
        );

        $consoleProcessorId = $providerServiceId . '.console_output.processor';
        $builder->setDefinition(
            $consoleProcessorId,
            new Definition(ConsoleOutputLogProcessor::class, [$levelsDefinition]),
        );

        $subscriberDefinition = new Definition(ConsoleLogOutputSubscriber::class, [new Reference($consoleProcessorId)]);
        $subscriberDefinition->addTag('kernel.event_subscriber');
        $builder->setDefinition($providerServiceId . '.console_output.subscriber', $subscriberDefinition);

        $compositeId = $exportProcessorServiceId . '.with_console_output';
        $compositeDefinition = new Definition(CompositeLogProcessor::class);
        $compositeDefinition->setArgument(0, [new Reference($exportProcessorServiceId), new Reference($consoleProcessorId)]);
        $compositeDefinition->setArgument(1, $errorHandlerRef);
        $builder->setDefinition($compositeId, $compositeDefinition);

        return $compositeId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildLogProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $builder): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        switch ($type) {
            case 'service':
                // @mago-expect analysis:mixed-assignment
                $customServiceId = $config['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $builder->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $builder->setDefinition($processorServiceId, new Definition(VoidLogProcessor::class));

                break;

            case 'memory':
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $builder);
                $definition = new Definition(MemoryLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $builder);
                $definition = new Definition(BatchingLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $definition->setArgument(3, $config['max_batch_age'] ?? null);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('log', $config['exporter'] ?? null, $builder);
                $definition = new Definition(PassThroughLogProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = is_array($config['processors'] ?? null) ? $config['processors'] : [];
                $processorRefs = [];

                // @mago-expect analysis:mixed-assignment
                foreach ($processors as $idx => $processorConfig) {
                    $subProcessorConfig = is_array($processorConfig) ? $processorConfig : [];
                    $subProcessorId = $this->buildLogProcessor(
                        $subProcessorConfig,
                        $processorServiceId . '.' . $idx,
                        $builder,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeLogProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'pipeline':
                $middlewareConfigs = is_array($config['middleware'] ?? null) ? $config['middleware'] : [];
                $middlewareRefs = [];

                // @mago-expect analysis:mixed-assignment
                foreach ($middlewareConfigs as $idx => $middlewareConfig) {
                    $middlewareRefs[] = new Reference($this->buildLogMiddleware(
                        is_array($middlewareConfig) ? $middlewareConfig : [],
                        $processorServiceId . '.middleware.' . $idx,
                        $builder,
                    ));
                }

                $sinkConfig = is_array($config['sink'] ?? null) ? $config['sink'] : [];
                $sinkServiceId = $this->buildLogProcessor($sinkConfig, $processorServiceId . '.sink', $builder);

                $definition = new Definition(PipelineLogProcessor::class);
                $definition->setArgument(0, $middlewareRefs);
                $definition->setArgument(1, new Reference($sinkServiceId));
                $builder->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown log processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildMeterProvider(array $config, ContainerBuilder $builder): string
    {
        $providerServiceId = 'flow.telemetry.meter_provider';

        $processorConfig = is_array($config['processor'] ?? null) ? $config['processor'] : [];
        $processorServiceId = $this->buildMetricProcessor($processorConfig, $providerServiceId, $builder);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        $temporality = ($config['temporality'] ?? 'cumulative') === 'delta'
            ? AggregationTemporality::DELTA
            : AggregationTemporality::CUMULATIVE;

        $definition = new Definition(MeterProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, $temporality);
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $builder->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildMetricProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $builder): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        switch ($type) {
            case 'service':
                // @mago-expect analysis:mixed-assignment
                $customServiceId = $config['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $builder->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $builder->setDefinition($processorServiceId, new Definition(VoidMetricProcessor::class));

                break;

            case 'memory':
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $builder);
                $definition = new Definition(MemoryMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $builder);
                $definition = new Definition(BatchingMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $definition->setArgument(3, $config['max_batch_age'] ?? null);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('metric', $config['exporter'] ?? null, $builder);
                $definition = new Definition(PassThroughMetricProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = is_array($config['processors'] ?? null) ? $config['processors'] : [];
                $processorRefs = [];

                // @mago-expect analysis:mixed-assignment
                foreach ($processors as $idx => $processorConfig) {
                    $subProcessorConfig = is_array($processorConfig) ? $processorConfig : [];
                    $subProcessorId = $this->buildMetricProcessor(
                        $subProcessorConfig,
                        $processorServiceId . '.' . $idx,
                        $builder,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeMetricProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'attribute_filtering':
                $innerProcessorConfig = is_array($config['inner_processor'] ?? null) ? $config['inner_processor'] : [];
                $innerProcessorServiceId = $this->buildMetricProcessor(
                    $innerProcessorConfig,
                    $processorServiceId . '.inner',
                    $builder,
                );
                $filterServiceId = $this->buildAttributeFilter($config, $processorServiceId, $builder);
                $definition = new Definition(AttributeFilteringMetricProcessor::class);
                $definition->setArgument(0, new Reference($innerProcessorServiceId));
                $definition->setArgument(1, new Reference($filterServiceId));
                $builder->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown metric processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildSampler(array $config, ContainerBuilder $builder): string
    {
        $samplerServiceId = 'flow.telemetry.tracer_provider.sampler';
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? 'always_on';

        switch ($type) {
            case 'service':
                // @mago-expect analysis:mixed-assignment
                $customServiceId = $config['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException('service_id is required when sampler type is "service"');
                }
                $builder->setAlias($samplerServiceId, $customServiceId);

                break;

            case 'always_on':
                $builder->setDefinition($samplerServiceId, new Definition(AlwaysOnSampler::class));

                break;

            case 'always_off':
                $builder->setDefinition($samplerServiceId, new Definition(AlwaysOffSampler::class));

                break;

            case 'trace_id_ratio':
                $definition = new Definition(TraceIdRatioBasedSampler::class);
                $definition->setArgument(0, $config['ratio'] ?? 1.0);
                $builder->setDefinition($samplerServiceId, $definition);

                break;

            case 'parent_based':
                $rootSamplerServiceId = $samplerServiceId . '.root';
                $rootSamplerDefinition = new Definition(AlwaysOnSampler::class);
                $builder->setDefinition($rootSamplerServiceId, $rootSamplerDefinition);

                $definition = new Definition(ParentBasedSampler::class);
                $definition->setArgument(0, new Reference($rootSamplerServiceId));
                $builder->setDefinition($samplerServiceId, $definition);

                break;

            case 'attribute_matching':
                $filterServiceId = $this->buildAttributeFilter($config, $samplerServiceId, $builder);
                $delegateConfig = is_array($config['delegate'] ?? null) ? $config['delegate'] : [];
                $delegateServiceId = $this->buildDelegateSampler(
                    $delegateConfig,
                    $samplerServiceId . '.delegate',
                    $builder,
                );
                $definition = new Definition(AttributeMatchingSampler::class);
                $definition->setArgument(0, new Reference($filterServiceId));
                $definition->setArgument(1, new Reference($delegateServiceId));
                $builder->setDefinition($samplerServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown sampler type: %s', (string) $type));
        }

        return $samplerServiceId;
    }

    /**
     * Build the leaf delegate sampler (always_on / always_off / trace_id_ratio) for an
     * attribute_matching sampler and return its service id.
     *
     * @param array<array-key, mixed> $config
     */
    private function buildDelegateSampler(array $config, string $serviceId, ContainerBuilder $builder): string
    {
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? 'always_on';

        if ($type === 'trace_id_ratio') {
            $definition = new Definition(TraceIdRatioBasedSampler::class);
            $definition->setArgument(0, $config['ratio'] ?? 1.0);
        } elseif ($type === 'always_off') {
            $definition = new Definition(AlwaysOffSampler::class);
        } else {
            $definition = new Definition(AlwaysOnSampler::class);
        }

        $builder->setDefinition($serviceId, $definition);

        return $serviceId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildSpanProcessor(array $config, string $serviceIdPrefix, ContainerBuilder $builder): string
    {
        $processorServiceId = $serviceIdPrefix . '.processor';
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? 'void';
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        switch ($type) {
            case 'service':
                // @mago-expect analysis:mixed-assignment
                $customServiceId = $config['service_id'] ?? null;

                if (!is_string($customServiceId) || $customServiceId === '') {
                    throw new RuntimeException('service_id is required when processor type is "service"');
                }
                $builder->setAlias($processorServiceId, $customServiceId);

                break;

            case 'void':
                $builder->setDefinition($processorServiceId, new Definition(VoidSpanProcessor::class));

                break;

            case 'memory':
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $builder);
                $definition = new Definition(MemorySpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'batching':
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $builder);
                $definition = new Definition(BatchingSpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $config['batch_size'] ?? 512);
                $definition->setArgument(2, $errorHandlerRef);
                $definition->setArgument(3, $config['max_batch_age'] ?? null);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'passthrough':
                $exporterRef = $this->resolveExporterReference('span', $config['exporter'] ?? null, $builder);
                $definition = new Definition(PassThroughSpanProcessor::class);
                $definition->setArgument(0, $exporterRef);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'composite':
                $processors = is_array($config['processors'] ?? null) ? $config['processors'] : [];
                $processorRefs = [];

                // @mago-expect analysis:mixed-assignment
                foreach ($processors as $idx => $processorConfig) {
                    $subProcessorConfig = is_array($processorConfig) ? $processorConfig : [];
                    $subProcessorId = $this->buildSpanProcessor(
                        $subProcessorConfig,
                        $processorServiceId . '.' . $idx,
                        $builder,
                    );
                    $processorRefs[] = new Reference($subProcessorId);
                }
                $definition = new Definition(CompositeSpanProcessor::class);
                $definition->setArgument(0, $processorRefs);
                $definition->setArgument(1, $errorHandlerRef);
                $builder->setDefinition($processorServiceId, $definition);

                break;

            case 'attribute_filtering':
                $innerProcessorConfig = is_array($config['inner_processor'] ?? null) ? $config['inner_processor'] : [];
                $innerProcessorServiceId = $this->buildSpanProcessor(
                    $innerProcessorConfig,
                    $processorServiceId . '.inner',
                    $builder,
                );
                $filterServiceId = $this->buildAttributeFilter($config, $processorServiceId, $builder);
                $definition = new Definition(AttributeFilteringSpanProcessor::class);
                $definition->setArgument(0, new Reference($innerProcessorServiceId));
                $definition->setArgument(1, new Reference($filterServiceId));
                $builder->setDefinition($processorServiceId, $definition);

                break;

            default:
                throw new RuntimeException(sprintf('Unknown span processor type: %s', (string) $type));
        }

        return $processorServiceId;
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function buildTracerProvider(array $config, ContainerBuilder $builder): string
    {
        $providerServiceId = 'flow.telemetry.tracer_provider';

        $processorConfig = is_array($config['processor'] ?? null) ? $config['processor'] : [];
        $samplerConfig = is_array($config['sampler'] ?? null) ? $config['sampler'] : [];
        $processorServiceId = $this->buildSpanProcessor($processorConfig, $providerServiceId, $builder);
        $samplerServiceId = $this->buildSampler($samplerConfig, $builder);
        $errorHandlerRef = $this->resolveErrorHandlerReference($config['error_handler'] ?? 'default', $builder);

        // Compose the configured sampler with tracing-suppression enforcement (the context-scoped
        // OpenTelemetry key) so suppression takes precedence over the sampling strategy in use.
        $suppressingSamplerServiceId = $samplerServiceId . '.suppressing';
        $builder->setDefinition(
            $suppressingSamplerServiceId,
            new Definition(SuppressingSampler::class, [new Reference($samplerServiceId)]),
        );

        $definition = new Definition(TracerProvider::class);
        $definition->setArgument(0, new Reference($processorServiceId));
        $definition->setArgument(1, new Reference('flow.telemetry.clock'));
        $definition->setArgument(2, new Reference('flow.telemetry.context_storage'));
        $definition->setArgument(3, new Reference($suppressingSamplerServiceId));
        $definition->setArgument('$errorHandler', $errorHandlerRef);
        $builder->setDefinition($providerServiceId, $definition);

        return $providerServiceId;
    }

    private function errorHandlersNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('error_handlers');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $supportedTypes = ['error_log', 'stream', 'syslog', 'udp_syslog', 'composite', 'noop', 'service'];
        $facilities = [
            'auth',
            'cron',
            'daemon',
            'kernel',
            'local0',
            'local1',
            'local2',
            'local3',
            'local4',
            'local5',
            'local6',
            'local7',
            'lpr',
            'mail',
            'news',
            'syslog',
            'user',
            'uucp',
        ];
        $severities = ['alert', 'critical', 'debug', 'emergency', 'error', 'info', 'notice', 'warning'];
        $messageTypes = ['operating_system', 'email', 'file', 'sapi'];

        $node
            ->info(
                'Named error handler definitions referenced by providers, processors, and OTLP exporters via "error_handler:" fields. If "default" is omitted it is auto-created with type: error_log.',
            )
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
            ->defaultValue(LOG_PID)
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
            ->scalarPrototype()
            ->end()
            ->end()
            ->scalarNode('service_id')
            ->info('Custom error handler service ID (only for type: service)')
            ->defaultNull()
            ->end()
            ->end()
            ->end();

        return $node;
    }

    private function exportersNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('exporters');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $supportedTypes = ['otlp', 'service', 'console', 'memory', 'void'];

        $node
            ->info(
                'Named exporter definitions referenced from per-signal processor blocks. The sub-block under each name selects the exporter implementation; "otlp" carries an embedded transport, "service" aliases an external service id.',
            )
            ->useAttributeAsKey('name')
            ->arrayPrototype()
            ->validate()
            ->ifTrue(static function (array $v) use ($supportedTypes): bool {
                $set = 0;

                foreach ($supportedTypes as $type) {
                    if (array_key_exists($type, $v) && $v[$type] !== null) {
                        $set++;
                    }
                }

                return $set !== 1;
            })
            ->thenInvalid('Exporter must declare exactly one of: otlp, service, console, memory, void.')
            ->end()
            ->children()
            ->booleanNode('enabled')
            ->info('When false, this exporter is replaced by a no-op (void) exporter: nothing is exported to its backend, while profiler capture and every other exporter keep working. Set it per environment to keep telemetry in the profiler without shipping it to a collector. Defaults to true.')
            ->defaultTrue()
            ->end()
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

    private function innerProcessorNode(string $signalType): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('inner_processor');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $childProcessorTypes = ['memory', 'batching', 'passthrough', 'void', 'service'];

        $node
            ->info('Inner (wrapped) processor for severity_filtering / attribute_filtering')
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
            ->floatNode('max_batch_age')
            ->info('Max batch age in seconds for batching processor; export at least this often in long-running processes (null = disabled)')
            ->defaultNull()
            ->min(0)
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

    /**
     * Build the matcher tree and AttributeFilter for an attribute_filtering processor
     * and return the filter service id.
     *
     * @param array<array-key, mixed> $config
     */
    private function buildAttributeFilter(array $config, string $serviceIdPrefix, ContainerBuilder $builder): string
    {
        if (!array_key_exists('matcher', $config) || $config['matcher'] === null) {
            throw new RuntimeException('attribute_filtering processor requires a "matcher"');
        }

        $matcherRef = $this->buildMatcher($config['matcher'], $serviceIdPrefix . '.matcher', $builder);

        $sources = is_array($config['sources'] ?? null) && $config['sources'] !== [] ? $config['sources'] : ['signal'];
        $sourceEnums = [];

        // @mago-expect analysis:mixed-assignment
        foreach ($sources as $source) {
            $sourceEnums[] = $this->mapAttributeSource(is_string($source) ? $source : 'signal');
        }

        // @mago-expect analysis:mixed-assignment
        $cacheDir = $config['cache_dir'] ?? null;
        // @mago-expect analysis:mixed-assignment
        $cacheDirPermissions = $config['cache_dir_permissions'] ?? 0o700;

        $filterServiceId = $serviceIdPrefix . '.filter';
        $filterDefinition = new Definition(AttributeFilter::class);
        $filterDefinition->setArgument(0, $matcherRef);
        $filterDefinition->setArgument(1, ($config['exclude'] ?? true) === true);
        $filterDefinition->setArgument(2, $sourceEnums);
        $filterDefinition->setArgument(
            3,
            is_string($cacheDir) && $cacheDir !== '' ? $cacheDir : '%kernel.cache_dir%/flow_telemetry_filters',
        );
        $filterDefinition->setArgument(4, is_int($cacheDirPermissions) ? $cacheDirPermissions : 0o700);
        $builder->setDefinition($filterServiceId, $filterDefinition);

        return $filterServiceId;
    }

    /**
     * Recursively build a matcher tree from config and return a reference to its
     * root service. A node is either a composite - exactly one of `all`/`any`
     * (a list of child matchers) or `not` (a single child matcher) - or a leaf
     * rule carrying a `path`. The four kinds are mutually exclusive.
     *
     * @param mixed $node
     */
    private function buildMatcher(mixed $node, string $serviceId, ContainerBuilder $builder): Reference
    {
        if (!is_array($node)) {
            throw new RuntimeException('attribute_filtering matcher must be a map: one of "all", "any", "not", or a rule with a "path"');
        }

        $kinds = [];

        foreach (['all', 'any', 'not', 'path'] as $key) {
            if (array_key_exists($key, $node)) {
                $kinds[] = $key;
            }
        }

        if (count($kinds) !== 1) {
            throw new RuntimeException(sprintf(
                'attribute_filtering matcher node must contain exactly one of "all", "any", "not" or "path", got: %s',
                $kinds === [] ? 'none' : implode(', ', $kinds),
            ));
        }

        if ($kinds[0] === 'all' || $kinds[0] === 'any') {
            // @mago-expect analysis:mixed-assignment
            $children = is_array($node[$kinds[0]]) ? array_values($node[$kinds[0]]) : [];

            if ($children === []) {
                throw new RuntimeException(sprintf('attribute_filtering "%s" matcher requires at least one child matcher', $kinds[0]));
            }

            $childRefs = [];

            // @mago-expect analysis:mixed-assignment
            foreach ($children as $idx => $child) {
                $childRefs[] = $this->buildMatcher($child, $serviceId . '.' . $idx, $builder);
            }

            $definition = new Definition($kinds[0] === 'all' ? All::class : Any::class);
            $definition->setArguments($childRefs);
            $builder->setDefinition($serviceId, $definition);

            return new Reference($serviceId);
        }

        if ($kinds[0] === 'not') {
            $definition = new Definition(Not::class);
            $definition->setArgument(0, $this->buildMatcher($node['not'], $serviceId . '.0', $builder));
            $builder->setDefinition($serviceId, $definition);

            return new Reference($serviceId);
        }

        // @mago-expect analysis:mixed-assignment
        $rawPath = $node['path'];

        if (is_array($rawPath)) {
            $path = array_values($rawPath);
        } elseif (is_string($rawPath)) {
            $path = $rawPath;
        } else {
            throw new RuntimeException('attribute_filtering matcher rule "path" must be a string or a list of strings');
        }

        $mode = is_string($node['mode'] ?? null) ? $node['mode'] : '';

        $definition = new Definition(AttributeRule::class);
        $definition->setArgument(0, $path);
        $definition->setArgument(1, $this->mapMatchMode($mode));
        $definition->setArgument(2, $node['value'] ?? '');
        $definition->setArgument(3, ($node['case_sensitive'] ?? true) === true);
        $builder->setDefinition($serviceId, $definition);

        return new Reference($serviceId);
    }

    /**
     * Build a single log pipeline middleware (enriching / attribute_filtering /
     * severity_filtering) and return its service id.
     *
     * @param array<array-key, mixed> $config
     */
    private function buildLogMiddleware(array $config, string $serviceId, ContainerBuilder $builder): string
    {
        // @mago-expect analysis:mixed-assignment
        $type = $config['type'] ?? '';

        switch ($type) {
            case 'enriching':
                $attributes = is_array($config['attributes'] ?? null) ? $config['attributes'] : [];
                $attributesDefinition = new Definition(Attributes::class);
                $attributesDefinition->setFactory([Attributes::class, 'create']);
                $attributesDefinition->setArgument(0, $attributes);
                $definition = new Definition(EnrichingLogMiddleware::class);
                $definition->setArgument(0, $attributesDefinition);

                break;

            case 'attribute_filtering':
                $filterServiceId = $this->buildAttributeFilter($config, $serviceId, $builder);
                $definition = new Definition(AttributeFilteringLogMiddleware::class);
                $definition->setArgument(0, new Reference($filterServiceId));

                break;

            case 'severity_filtering':
                // @mago-expect analysis:mixed-assignment
                $minSeverity = $config['minimum_severity'] ?? 'info';
                $definition = new Definition(SeverityFilteringLogMiddleware::class);
                $definition->setArgument(0, $this->mapSeverity(is_string($minSeverity) ? $minSeverity : 'info'));

                break;

            default:
                throw new RuntimeException(sprintf('Unknown log middleware type: %s', (string) $type));
        }

        $builder->setDefinition($serviceId, $definition);

        return $serviceId;
    }

    private function mapMatchMode(string $mode): MatchMode
    {
        return match ($mode) {
            'equal' => MatchMode::EQUAL,
            'not_equal' => MatchMode::NOT_EQUAL,
            'greater_than' => MatchMode::GREATER_THAN,
            'greater_than_equal' => MatchMode::GREATER_THAN_EQUAL,
            'less_than' => MatchMode::LESS_THAN,
            'less_than_equal' => MatchMode::LESS_THAN_EQUAL,
            'regexp' => MatchMode::REGEXP,
            'starts_with' => MatchMode::STARTS_WITH,
            'ends_with' => MatchMode::ENDS_WITH,
            'contains' => MatchMode::CONTAINS,
            default => throw new RuntimeException(sprintf('Unknown attribute match mode: %s', $mode)),
        };
    }

    private function mapAttributeSource(string $source): AttributeSource
    {
        return match ($source) {
            'signal' => AttributeSource::SIGNAL,
            'resource' => AttributeSource::RESOURCE,
            'scope' => AttributeSource::SCOPE,
            default => throw new RuntimeException(sprintf('Unknown attribute source: %s', $source)),
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

    private function otlpExporterNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('otlp');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('OTLP exporter — embeds its transport configuration inline')
            ->canBeUnset()
            ->validate()
            ->ifTrue(static fn(array $v): bool => !is_array($v['transport'] ?? null) || count($v['transport']) === 0)
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

    private function processorNode(string $signalType): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('processor');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $processorTypes = ['composite', 'memory', 'batching', 'passthrough', 'void', 'service'];
        $childProcessorTypes = ['memory', 'batching', 'passthrough', 'void', 'service'];

        if ($signalType === 'log') {
            // Logs filter/enrich via a pipeline of middleware (see pipeline type);
            // attribute/severity filtering are middleware, not standalone log processors.
            $processorTypes[] = 'pipeline';
        } else {
            $processorTypes[] = 'attribute_filtering';
            $childProcessorTypes[] = 'attribute_filtering';
        }

        $node
            ->info(ucfirst($signalType) . ' processor configuration')
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
            ->floatNode('max_batch_age')
            ->info('Max batch age in seconds for batching processor; export at least this often in long-running processes (null = disabled)')
            ->defaultNull()
            ->min(0)
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
            ->booleanNode('exclude')
            ->info('attribute_filtering: when true (default) a match drops the signal; when false only matching signals are kept')
            ->defaultTrue()
            ->end()
            ->arrayNode('sources')
            ->info('attribute_filtering: which attribute sets to inspect - any of signal (default), resource, scope. A signal matches if the matcher matches in ANY listed source.')
            ->enumPrototype()
            ->values(['signal', 'resource', 'scope'])
            ->end()
            ->defaultValue(['signal'])
            ->end()
            ->scalarNode('cache_dir')
            ->info('attribute_filtering: directory for the generated matcher cache. Defaults to %kernel.cache_dir%/flow_telemetry_filters')
            ->defaultNull()
            ->end()
            ->integerNode('cache_dir_permissions')
            ->info('attribute_filtering: octal mode applied when the matcher cache directory is created (default 0o700, owner-only; write it as a YAML octal literal e.g. 0o750)')
            ->defaultValue(0o700)
            ->min(0)
            ->max(0o777)
            ->end()
            ->variableNode('matcher')
            ->info('attribute_filtering: the matcher tree. A node is exactly one of: "all"/"any" (a list of child matchers), "not" (a single child matcher), or a leaf rule { path, mode, value, case_sensitive? }. Nestable to any depth.')
            ->defaultNull()
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
            ->floatNode('max_batch_age')
            ->info('Max batch age in seconds for batching processor; export at least this often in long-running processes (null = disabled)')
            ->defaultNull()
            ->min(0)
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
            ->booleanNode('exclude')
            ->defaultTrue()
            ->end()
            ->arrayNode('sources')
            ->enumPrototype()
            ->values(['signal', 'resource', 'scope'])
            ->end()
            ->defaultValue(['signal'])
            ->end()
            ->scalarNode('cache_dir')
            ->defaultNull()
            ->end()
            ->integerNode('cache_dir_permissions')
            ->defaultValue(0o700)
            ->min(0)
            ->max(0o777)
            ->end()
            ->variableNode('matcher')
            ->defaultNull()
            ->end()
            ->append($this->innerProcessorNode($signalType))
            ->end()
            ->end()
            ->end()
            ->append($this->innerProcessorNode($signalType))
            ->append($this->logMiddlewareNode())
            ->append($this->sinkNode())
            ->end();

        return $node;
    }

    /**
     * Ordered list of log pipeline middleware. Only meaningful for the log `pipeline`
     * processor type; each entry is one of enriching / attribute_filtering / severity_filtering.
     */
    private function logMiddlewareNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('middleware');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('Ordered log middleware (only for type: pipeline)')
            ->arrayPrototype()
            ->children()
            ->enumNode('type')
            ->values(['enriching', 'attribute_filtering', 'severity_filtering'])
            ->isRequired()
            ->end()
            ->arrayNode('attributes')
            ->info('enriching: default attributes merged into every record (call-site values win)')
            ->normalizeKeys(false)
            ->useAttributeAsKey('name')
            ->prototype('variable')
            ->end()
            ->end()
            ->enumNode('minimum_severity')
            ->info('severity_filtering: minimum severity level')
            ->values(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
            ->defaultValue('info')
            ->end()
            ->booleanNode('exclude')
            ->info('attribute_filtering: when true (default) a match drops the entry; when false only matching entries are kept')
            ->defaultTrue()
            ->end()
            ->arrayNode('sources')
            ->info('attribute_filtering: which attribute sets to inspect - any of signal (default), resource, scope')
            ->enumPrototype()
            ->values(['signal', 'resource', 'scope'])
            ->end()
            ->defaultValue(['signal'])
            ->end()
            ->scalarNode('cache_dir')
            ->info('attribute_filtering: directory for the generated matcher cache. Defaults to %kernel.cache_dir%/flow_telemetry_filters')
            ->defaultNull()
            ->end()
            ->integerNode('cache_dir_permissions')
            ->info('attribute_filtering: octal mode applied when the matcher cache directory is created (default 0o700, owner-only; write it as a YAML octal literal e.g. 0o750)')
            ->defaultValue(0o700)
            ->min(0)
            ->max(0o777)
            ->end()
            ->variableNode('matcher')
            ->info('attribute_filtering: the matcher tree (all/any/not or a leaf rule { path, mode, value, case_sensitive? }); nestable to any depth')
            ->defaultNull()
            ->end()
            ->end()
            ->end();

        return $node;
    }

    /**
     * Terminal processor (sink) for a log pipeline. Only meaningful for the log
     * `pipeline` processor type.
     */
    private function sinkNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('sink');
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node
            ->info('Terminal processor for the pipeline (only for type: pipeline)')
            ->children()
            ->enumNode('type')
            ->values(['composite', 'memory', 'batching', 'passthrough', 'void', 'service'])
            ->isRequired()
            ->end()
            ->integerNode('batch_size')
            ->defaultValue(512)
            ->min(1)
            ->end()
            ->floatNode('max_batch_age')
            ->info('Max batch age in seconds for batching processor; export at least this often in long-running processes (null = disabled)')
            ->defaultNull()
            ->min(0)
            ->end()
            ->scalarNode('exporter')
            ->defaultNull()
            ->end()
            ->scalarNode('service_id')
            ->defaultNull()
            ->end()
            ->scalarNode('error_handler')
            ->defaultValue('default')
            ->end()
            ->arrayNode('processors')
            ->info('Child processors (only for sink type: composite)')
            ->arrayPrototype()
            ->children()
            ->enumNode('type')
            ->values(['memory', 'batching', 'passthrough', 'void', 'service'])
            ->isRequired()
            ->end()
            ->integerNode('batch_size')
            ->defaultValue(512)
            ->min(1)
            ->end()
            ->floatNode('max_batch_age')
            ->info('Max batch age in seconds for batching processor; export at least this often in long-running processes (null = disabled)')
            ->defaultNull()
            ->min(0)
            ->end()
            ->scalarNode('exporter')
            ->defaultNull()
            ->end()
            ->scalarNode('service_id')
            ->defaultNull()
            ->end()
            ->scalarNode('error_handler')
            ->defaultValue('default')
            ->end()
            ->end()
            ->end()
            ->end();

        return $node;
    }

    /**
     * @param array<string, array<string, mixed>> $config
     */
    private function registerErrorHandlers(array $config, ContainerBuilder $builder): void
    {
        if (!array_key_exists('default', $config)) {
            $config = ['default' => ['type' => 'error_log']] + $config;
        }

        $compositeNames = [];

        foreach ($config as $name => $handlerConfig) {
            // @mago-expect analysis:mixed-assignment
            $type = $handlerConfig['type'] ?? 'error_log';

            if ($type === 'composite') {
                $compositeNames[] = $name;

                continue;
            }

            $this->buildErrorHandlerDefinition($name, $handlerConfig, $builder);
        }

        foreach ($compositeNames as $name) {
            $this->buildErrorHandlerDefinition($name, $config[$name], $builder);
        }
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function registerGlobalServices(array $config, ContainerBuilder $builder): void
    {
        // @mago-expect analysis:mixed-assignment
        $clockServiceId = $config['clock_service_id'] ?? null;

        if (is_string($clockServiceId)) {
            $builder->setAlias('flow.telemetry.clock', $clockServiceId);
        } elseif ($builder->has(ClockInterface::class)) {
            $builder->setAlias('flow.telemetry.clock', ClockInterface::class);
        } else {
            $builder->setDefinition('flow.telemetry.clock', new Definition(SystemClock::class));
        }

        $contextStorageConfig = is_array($config['context_storage'] ?? null) ? $config['context_storage'] : [];
        $contextStorageType = is_string($contextStorageConfig['type'] ?? null) ? $contextStorageConfig['type'] : 'memory';

        if ($contextStorageType === 'service') {
            // @mago-expect analysis:mixed-assignment
            $customServiceId = $contextStorageConfig['service_id'] ?? null;

            if (!is_string($customServiceId) || $customServiceId === '') {
                throw new RuntimeException('service_id is required when context_storage type is "service"');
            }
            $builder->setAlias('flow.telemetry.context_storage', $customServiceId);
        } else {
            $builder->setDefinition('flow.telemetry.context_storage', new Definition(MemoryContextStorage::class));
        }

        $runtimeMode = is_string($config['runtime_mode'] ?? null) ? $config['runtime_mode'] : 'auto';

        $builder->setDefinition('flow.telemetry.worker_mode_detector', new Definition(EnvironmentWorkerModeDetector::class));
        $builder->setAlias(WorkerModeDetector::class, 'flow.telemetry.worker_mode_detector');

        $builder->setDefinition('flow.telemetry.runtime_mode_resolver', new Definition(RuntimeModeResolver::class, [
            $runtimeMode,
            new Reference('flow.telemetry.worker_mode_detector'),
        ]));

        $builder->setDefinition(
            'flow.telemetry.psr3.log_record_converter',
            new Definition(LogRecordConverter::class),
        );
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool, trace_controller?: bool, trace_controller_resolution?: bool, trace_controller_arguments?: bool, trace_controller_argument_resolvers?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool, trace?: bool, metrics?: bool, metrics_duration_unit?: 's'|'ms'}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, max_sql_length?: int, collect_metrics?: bool, include_parameters?: bool, max_parameters?: int, max_parameter_length?: int, transaction_spans?: 'grouped'|'per_operation'|'off', exclude_connections?: array<string>, exclude_tables?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>, flush_deferred?: bool}} $config
     */
    private function registerInstrumentation(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $httpKernelConfig = $config['http_kernel'] ?? [];

        if ((bool) ($httpKernelConfig['enabled'] ?? false)) {
            $builder->setParameter(
                'flow.telemetry.http_kernel.exclude_paths',
                $httpKernelConfig['exclude_paths'] ?? [],
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.context_propagation',
                ($httpKernelConfig['context_propagation'] ?? true) && class_exists(self::HTTP_FOUNDATION_REQUEST_CARRIER),
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.context_propagation_query',
                ($httpKernelConfig['context_propagation_query'] ?? false) && class_exists(self::HTTP_FOUNDATION_REQUEST_CARRIER),
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.trace_controller',
                $httpKernelConfig['trace_controller'] ?? true,
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.trace_controller_resolution',
                $httpKernelConfig['trace_controller_resolution'] ?? false,
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.trace_controller_arguments',
                $httpKernelConfig['trace_controller_arguments'] ?? false,
            );
            $builder->setParameter(
                'flow.telemetry.http_kernel.trace_controller_argument_resolvers',
                $httpKernelConfig['trace_controller_argument_resolvers'] ?? false,
            );
            $container->import(__DIR__ . '/Resources/config/instrumentation/http_kernel.php');

            $routeNaming = is_string($httpKernelConfig['route_naming'] ?? null)
                ? $httpKernelConfig['route_naming']
                : 'path';
            $builder->getDefinition('flow.telemetry.http_kernel.span_subscriber')
                ->setArgument('$routeNaming', RouteNaming::from($routeNaming));
        }

        $consoleConfig = $config['console'] ?? [];
        $excludeCommands = $consoleConfig['exclude_commands'] ?? ['messenger:consume'];

        if ((bool) ($consoleConfig['enabled'] ?? false)) {
            $builder->setParameter('flow.telemetry.console.exclude_commands', $excludeCommands);
            $container->import(__DIR__ . '/Resources/config/instrumentation/console.php');
        }

        // Excluded commands are fully suppressed (the command and everything nested under it). Registered
        // independently of console spans so worker suppression still applies when console tracing is off;
        // an empty exclude list traces everything, including messenger:consume.
        if ($excludeCommands !== []) {
            $suppressionSubscriber = new Definition(CommandSuppressionSubscriber::class);
            $suppressionSubscriber->setArgument(0, new Reference('flow.telemetry.context_storage'));
            $suppressionSubscriber->setArgument(1, $excludeCommands);
            $suppressionSubscriber->addTag('kernel.event_subscriber');
            $builder->setDefinition(
                'flow.telemetry.console.command_suppression_subscriber',
                $suppressionSubscriber,
            );
        }

        $messengerConfig = $config['messenger'] ?? [];

        if ((bool) ($messengerConfig['enabled'] ?? false)) {
            if (!interface_exists(self::MESSENGER_MIDDLEWARE_INTERFACE)) {
                throw new RuntimeException(
                    'Messenger instrumentation requires symfony/messenger package. Install it via composer: composer require symfony/messenger',
                );
            }

            $container->import(__DIR__ . '/Resources/config/instrumentation/messenger.php');

            $definition = $builder->getDefinition('flow.telemetry.messenger.middleware');
            $definition->setArgument(1, new Reference('flow.telemetry.context_storage'));
            $definition->setArgument(2, ($messengerConfig['context_propagation'] ?? true)
                ? new Reference('flow.telemetry.propagator')
                : null);
            $definition->setArgument(3, ($messengerConfig['trace'] ?? true) === true);
            $definition->setArgument(4, ($messengerConfig['metrics'] ?? true) === true);
            $definition->setArgument(5, MessengerMetricDurationUnit::from($messengerConfig['metrics_duration_unit'] ?? 's'));
        }

        $twigConfig = $config['twig'] ?? [];

        if ((bool) ($twigConfig['enabled'] ?? false)) {
            if (!class_exists(AbstractExtension::class)) {
                throw new RuntimeException(
                    'Twig instrumentation requires twig/twig package. Install it via composer: composer require twig/twig',
                );
            }

            $builder->setParameter('flow.telemetry.twig.trace_templates', $twigConfig['trace_templates'] ?? true);
            $builder->setParameter('flow.telemetry.twig.trace_blocks', $twigConfig['trace_blocks'] ?? false);
            $builder->setParameter('flow.telemetry.twig.trace_macros', $twigConfig['trace_macros'] ?? false);
            $builder->setParameter('flow.telemetry.twig.exclude_templates', $twigConfig['exclude_templates'] ?? []);
            $container->import(__DIR__ . '/Resources/config/instrumentation/twig.php');
        }

        $securityConfig = $config['security'] ?? [];

        if ((bool) ($securityConfig['enabled'] ?? false)) {
            if (!interface_exists(self::SECURITY_TOKEN_STORAGE_INTERFACE)) {
                throw new RuntimeException(
                    'Security instrumentation requires symfony/security-core package. Install it via composer: composer require symfony/security-core',
                );
            }

            $fields = is_array($securityConfig['fields'] ?? null) ? $securityConfig['fields'] : [];
            $idField = is_array($fields['id'] ?? null) ? $fields['id'] : [];
            $rolesField = is_array($fields['roles'] ?? null) ? $fields['roles'] : [];
            $emailField = is_array($fields['email'] ?? null) ? $fields['email'] : [];

            $builder->setParameter(
                'flow.telemetry.security.field.id_attribute',
                ($idField['enabled'] ?? true) === true ? ($idField['attribute'] ?? 'user.id') : null,
            );
            $builder->setParameter(
                'flow.telemetry.security.field.roles_attribute',
                ($rolesField['enabled'] ?? false) === true ? ($rolesField['attribute'] ?? 'user.roles') : null,
            );
            $builder->setParameter(
                'flow.telemetry.security.field.email_attribute',
                ($emailField['enabled'] ?? false) === true ? ($emailField['attribute'] ?? 'user.email') : null,
            );
            $builder->setParameter(
                'flow.telemetry.security.field.email_getter',
                $emailField['getter'] ?? 'getEmail',
            );

            $container->import(__DIR__ . '/Resources/config/instrumentation/security.php');
        }

        // Outgoing trace-context helpers are not instrumentation; register them alongside it.
        $container->import(__DIR__ . '/Resources/config/propagation.php');

        if (class_exists(AbstractExtension::class)) {
            $container->import(__DIR__ . '/Resources/config/twig_propagation.php');
        }

        if (interface_exists(self::URL_GENERATOR_INTERFACE)) {
            $container->import(__DIR__ . '/Resources/config/url_propagation.php');
        }

        $this->registerParameterOnlyInstrumentation($config, $builder);
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function registerProfiler(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $profilerConfig = is_array($config['profiler'] ?? null) ? $config['profiler'] : [];
        // @mago-expect analysis:mixed-assignment
        $enabled = $profilerConfig['enabled'] ?? null;
        $captureLogs = (bool) ($profilerConfig['capture_logs'] ?? false);

        $hasWebProfiler = $this->isWebProfilerBundleRegistered($builder);

        if ($enabled === false) {
            return;
        }

        if ($enabled === true && !$hasWebProfiler) {
            throw new RuntimeException(
                'Profiler integration requires symfony/web-profiler-bundle to be registered in the kernel. Install it (composer require --dev symfony/web-profiler-bundle) and enable it for this environment.',
            );
        }

        if ($enabled === null && !$hasWebProfiler) {
            return;
        }

        $tracerProviderConfig = is_array($config['tracer_provider'] ?? null) ? $config['tracer_provider'] : [];
        $meterProviderConfig = is_array($config['meter_provider'] ?? null) ? $config['meter_provider'] : [];

        $capturedIds = [];

        foreach ($this->collectExporterServiceIds($tracerProviderConfig) as $id) {
            $capturedIds[$id] = $id;
        }

        foreach ($this->collectExporterServiceIds($meterProviderConfig) as $id) {
            $capturedIds[$id] = $id;
        }

        if ($captureLogs) {
            $loggerProviderConfig = is_array($config['logger_provider'] ?? null) ? $config['logger_provider'] : [];

            foreach ($this->collectExporterServiceIds($loggerProviderConfig) as $id) {
                $capturedIds[$id] = $id;
            }
        }

        $builder->setParameter('flow.telemetry.profiler.captured_exporters', array_values($capturedIds));
        $builder->setParameter('flow.telemetry.profiler.capture_logs', $captureLogs);
        $builder->setParameter('flow.telemetry.profiler.configured_instruments', $this->configuredInstruments($config));

        $container->import(__DIR__ . '/Resources/config/profiler.php');
    }

    /**
     * Flatten the named tracer/meter/logger config into rows for the profiler panel, so configured
     * scope attributes stay visible even when an instrument did not emit a signal during the request.
     *
     * @param array<array-key, mixed> $config
     *
     * @return list<array{type: string, name: string, version: string, attributes: array<string, mixed>}>
     */
    private function configuredInstruments(array $config): array
    {
        $rows = [];

        $groups = [
            'tracer' => $config['tracers'] ?? null,
            'meter' => $config['meters'] ?? null,
            'logger' => $config['loggers'] ?? null,
        ];

        foreach ($groups as $type => $instruments) {
            if (!is_array($instruments)) {
                continue;
            }

            foreach ($instruments as $name => $instrumentConfig) {
                if (!is_array($instrumentConfig)) {
                    continue;
                }

                $attributes = is_array($instrumentConfig['attributes'] ?? null) ? $instrumentConfig['attributes'] : [];

                $rows[] = [
                    'type' => $type,
                    'name' => (string) $name,
                    'version' => is_string($instrumentConfig['version'] ?? null) ? $instrumentConfig['version'] : 'unknown',
                    'attributes' => is_array($attributes['scope'] ?? null) ? $attributes['scope'] : [],
                ];
            }
        }

        return $rows;
    }

    /**
     * @param array<array-key, mixed> $providerConfig
     *
     * @return list<string>
     */
    private function collectExporterServiceIds(array $providerConfig): array
    {
        $processorConfig = is_array($providerConfig['processor'] ?? null) ? $providerConfig['processor'] : [];

        $ids = [];

        foreach ($this->collectExporterNames($processorConfig) as $name) {
            $ids[] = 'flow.telemetry.exporter.' . $name;
        }

        return $ids;
    }

    /**
     * @param array<array-key, mixed> $processorConfig
     *
     * @return list<string>
     */
    private function collectExporterNames(array $processorConfig): array
    {
        $names = [];

        // @mago-expect analysis:mixed-assignment
        $exporter = $processorConfig['exporter'] ?? null;

        if (is_string($exporter) && $exporter !== '') {
            $names[] = $exporter;
        }

        // @mago-expect analysis:mixed-assignment
        $children = $processorConfig['processors'] ?? null;

        // @mago-expect analysis:mixed-assignment
        foreach (is_array($children) ? $children : [] as $child) {
            if (is_array($child)) {
                $names = [...$names, ...$this->collectExporterNames($child)];
            }
        }

        foreach (['inner_processor', 'sink'] as $key) {
            // @mago-expect analysis:mixed-assignment
            $nested = $processorConfig[$key] ?? null;

            if (is_array($nested)) {
                $names = [...$names, ...$this->collectExporterNames($nested)];
            }
        }

        return $names;
    }

    /**
     * Auto-detect the profiler only when WebProfilerBundle is actually wired into the kernel, not
     * merely installed in vendor. Detection is by registered class (the "kernel.bundles" value),
     * which is stable across Symfony 6.4/7.4/8.0 regardless of the bundle's registration key.
     */
    private function isWebProfilerBundleRegistered(ContainerBuilder $builder): bool
    {
        if (!$builder->hasParameter('kernel.bundles')) {
            return false;
        }

        $bundles = $builder->getParameter('kernel.bundles');

        return is_array($bundles) && in_array(self::WEB_PROFILER_BUNDLE, $bundles, true);
    }

    /**
     * Register the named logger and its PSR-3 wrapper, returning the PSR-3 service id.
     *
     * Idempotent: a logger already defined under this name (e.g. declared via
     * configuration) is left untouched, so a channel synthesized by
     * {@see ChannelLoggerPass} never overrides a user-declared scope.
     *
     * @param array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}} $loggerConfig
     */
    public static function defineLogger(string $name, array $loggerConfig, ContainerBuilder $builder): string
    {
        $loggerServiceId = 'flow.telemetry.' . $name . '.logger';

        if (!$builder->hasDefinition($loggerServiceId)) {
            $definition = new Definition(Logger::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'logger']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $loggerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $loggerConfig['schema_url'] ?? null);

            self::applyScopeAndSignalAttributes($definition, $loggerConfig);

            $definition->setPublic(true);
            $builder->setDefinition($loggerServiceId, $definition);

            $psr3Definition = new Definition(TelemetryLogger::class);
            $psr3Definition->setArgument(0, new Reference($loggerServiceId));
            $psr3Definition->setArgument(1, new Reference('flow.telemetry.psr3.log_record_converter'));
            $psr3Definition->setPublic(true);
            $builder->setDefinition($loggerServiceId . '.psr3', $psr3Definition);
        }

        return $loggerServiceId . '.psr3';
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array<string, mixed>}> $config
     */
    private function registerLoggers(array $config, ContainerBuilder $builder): void
    {
        foreach ($config as $name => $loggerConfig) {
            self::defineLogger($name, $loggerConfig, $builder);
        }
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}}> $config
     */
    private function registerMeters(array $config, ContainerBuilder $builder): void
    {
        foreach ($config as $name => $meterConfig) {
            $definition = new Definition(Meter::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'meter']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $meterConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $meterConfig['schema_url'] ?? null);

            self::applyScopeAndSignalAttributes($definition, $meterConfig);

            $definition->setPublic(true);
            $builder->setDefinition('flow.telemetry.' . $name . '.meter', $definition);
        }
    }

    /**
     * Wire scope (constructor arg 3) and signal (constructor arg 4) attribute defaults onto a
     * tracer/meter/logger factory definition. Each is an {@see Attributes} service, or null when empty.
     *
     * @param array<array-key, mixed> $config a single tracer/meter/logger configuration entry
     */
    private static function applyScopeAndSignalAttributes(Definition $definition, array $config): void
    {
        $attributes = is_array($config['attributes'] ?? null) ? $config['attributes'] : [];

        $definition->setArgument(3, self::attributesArgument(
            is_array($attributes['scope'] ?? null) ? $attributes['scope'] : [],
        ));
        $definition->setArgument(4, self::attributesArgument(
            is_array($attributes['signal'] ?? null) ? $attributes['signal'] : [],
        ));
    }

    /**
     * @param array<array-key, mixed> $attributes
     */
    private static function attributesArgument(array $attributes): ?Definition
    {
        if ($attributes === []) {
            return null;
        }

        $definition = new Definition(Attributes::class);
        $definition->setFactory([Attributes::class, 'create']);
        $definition->setArgument(0, $attributes);

        return $definition;
    }

    /**
     * @param array<string, array<string, mixed>> $config
     */
    private function registerNamedExporters(array $config, ContainerBuilder $builder): void
    {
        foreach ($config as $name => $exporterConfig) {
            $serviceId = 'flow.telemetry.exporter.' . $name;
            // @mago-expect analysis:mixed-assignment
            $enabled = $exporterConfig['enabled'] ?? true;

            // A literal false is known at compile time, so skip building the real backend entirely.
            if ($enabled === false) {
                $builder->setDefinition($serviceId, new Definition(VoidExporter::class));

                continue;
            }

            $this->registerExporterDefinition($name, $serviceId, $exporterConfig, $builder);

            // Anything that is not a literal true is an unresolved env placeholder (e.g. %env(bool:...)%);
            // wrap the real exporter so the flag is honored at runtime instead of compile time.
            if ($enabled !== true) {
                $this->wrapExporterWithRuntimeToggle($serviceId, $enabled, $builder);
            }
        }
    }

    /**
     * @param array<string, mixed> $exporterConfig
     */
    private function registerExporterDefinition(
        string $name,
        string $serviceId,
        array $exporterConfig,
        ContainerBuilder $builder,
    ): void {
        if (array_key_exists('void', $exporterConfig)) {
            $builder->setDefinition($serviceId, new Definition(VoidExporter::class));

            return;
        }

        if (array_key_exists('memory', $exporterConfig)) {
            $builder->setDefinition($serviceId, new Definition(MemoryExporter::class));

            return;
        }

        if (array_key_exists('console', $exporterConfig)) {
            $builder->setDefinition($serviceId, new Definition(ConsoleExporter::class));

            return;
        }

        if (array_key_exists('service', $exporterConfig)) {
            // @mago-expect analysis:mixed-assignment
            $customServiceId = $exporterConfig['service']['id'] ?? null;

            if (!is_string($customServiceId) || $customServiceId === '') {
                throw new RuntimeException(sprintf('exporter "%s" of type "service" requires "service.id"', $name));
            }
            $builder->setAlias($serviceId, $customServiceId);

            return;
        }

        if (array_key_exists('otlp', $exporterConfig)) {
            $builder->setParameter('flow.telemetry.otlp_configured', true);
            // @mago-expect analysis:mixed-assignment
            $transportConfig = $exporterConfig['otlp']['transport'] ?? null;

            if (!is_array($transportConfig) || count($transportConfig) === 0) {
                throw new RuntimeException(sprintf(
                    'exporter "%s" of type "otlp" requires an inline "transport" configuration',
                    $name,
                ));
            }
            $errorHandlerRef = $this->resolveErrorHandlerReference(
                $exporterConfig['otlp']['error_handler'] ?? 'default',
                $builder,
            );
            $transportServiceId = $this->buildEmbeddedOtlpTransport(
                $name,
                $transportConfig,
                $builder,
                errorHandlerRef: $errorHandlerRef,
            );
            $definition = new Definition(OTLPExporter::class);
            $definition->setArgument(0, new Reference($transportServiceId));
            $definition->setArgument(1, $errorHandlerRef);
            $builder->setDefinition($serviceId, $definition);

            return;
        }

        throw new RuntimeException(sprintf(
            'exporter "%s" must declare exactly one of: otlp, service, console, memory, void',
            $name,
        ));
    }

    private function wrapExporterWithRuntimeToggle(string $serviceId, mixed $enabled, ContainerBuilder $builder): void
    {
        $innerId = $serviceId . '.toggle.inner';

        if ($builder->hasAlias($serviceId)) {
            $builder->setAlias($innerId, $builder->getAlias($serviceId));
            $builder->removeAlias($serviceId);
        } else {
            $builder->setDefinition($innerId, $builder->getDefinition($serviceId));
        }

        $definition = new Definition(ConditionalExporter::class);
        $definition->setArgument(0, $enabled);
        $definition->setArgument(1, new Reference($innerId));
        $builder->setDefinition($serviceId, $definition);
    }

    /**
     * @param array{http_kernel?: array{enabled?: bool, exclude_routes?: array<string>, exclude_paths?: array<array{path: string, method?: null|string}>, context_propagation?: bool, trace_controller?: bool, trace_controller_resolution?: bool, trace_controller_arguments?: bool, trace_controller_argument_resolvers?: bool}, console?: array{enabled?: bool, exclude_commands?: array<string>}, messenger?: array{enabled?: bool, context_propagation?: bool, trace?: bool, metrics?: bool, metrics_duration_unit?: 's'|'ms'}, twig?: array{enabled?: bool, trace_templates?: bool, trace_blocks?: bool, trace_macros?: bool, exclude_templates?: array<string>}, http_client?: array{enabled?: bool, exclude_clients?: array<string>}, psr18_client?: array{enabled?: bool, exclude_clients?: array<string>}, dbal?: array{enabled?: bool, max_sql_length?: int, collect_metrics?: bool, include_parameters?: bool, max_parameters?: int, max_parameter_length?: int, transaction_spans?: 'grouped'|'per_operation'|'off', exclude_connections?: array<string>, exclude_tables?: array<string>}, cache?: array{enabled?: bool, exclude_pools?: array<string>, flush_deferred?: bool}} $config
     */
    private function registerParameterOnlyInstrumentation(array $config, ContainerBuilder $builder): void
    {
        $httpClientConfig = $config['http_client'] ?? [];
        $builder->setParameter('flow.telemetry.http_client.enabled', $httpClientConfig['enabled'] ?? false);
        $builder->setParameter(
            'flow.telemetry.http_client.exclude_clients',
            $httpClientConfig['exclude_clients'] ?? [],
        );

        $psr18ClientConfig = $config['psr18_client'] ?? [];
        $builder->setParameter('flow.telemetry.psr18_client.enabled', $psr18ClientConfig['enabled'] ?? false);
        $builder->setParameter(
            'flow.telemetry.psr18_client.exclude_clients',
            $psr18ClientConfig['exclude_clients'] ?? [],
        );

        $dbalConfig = $config['dbal'] ?? [];
        $builder->setParameter('flow.telemetry.dbal.enabled', $dbalConfig['enabled'] ?? false);
        $builder->setParameter('flow.telemetry.dbal.max_sql_length', $dbalConfig['max_sql_length'] ?? 1000);
        $builder->setParameter('flow.telemetry.dbal.collect_metrics', $dbalConfig['collect_metrics'] ?? true);
        $builder->setParameter('flow.telemetry.dbal.include_parameters', $dbalConfig['include_parameters'] ?? false);
        $builder->setParameter('flow.telemetry.dbal.max_parameters', $dbalConfig['max_parameters'] ?? 10);
        $builder->setParameter('flow.telemetry.dbal.max_parameter_length', $dbalConfig['max_parameter_length'] ?? 100);
        $builder->setParameter('flow.telemetry.dbal.transaction_spans', $dbalConfig['transaction_spans'] ?? 'grouped');
        $builder->setParameter('flow.telemetry.dbal.exclude_connections', $dbalConfig['exclude_connections'] ?? []);
        $builder->setParameter('flow.telemetry.dbal.exclude_tables', $dbalConfig['exclude_tables'] ?? []);

        $cacheConfig = $config['cache'] ?? [];
        $builder->setParameter('flow.telemetry.cache.enabled', $cacheConfig['enabled'] ?? false);
        $builder->setParameter('flow.telemetry.cache.exclude_pools', $cacheConfig['exclude_pools'] ?? []);
        $builder->setParameter('flow.telemetry.cache.flush_deferred', $cacheConfig['flush_deferred'] ?? false);
    }

    /**
     * @param array{type?: string, service_id?: null|string} $config
     */
    private function registerPropagator(array $config, ContainerBuilder $builder): void
    {
        $type = $config['type'] ?? 'w3c';

        switch ($type) {
            case 'service':
                $customServiceId = $config['service_id'] ?? null;

                if ($customServiceId === null) {
                    throw new RuntimeException('service_id is required when propagator type is "service"');
                }
                $builder->setAlias('flow.telemetry.propagator', $customServiceId);

                break;

            case 'w3c':
                $builder->setDefinition(
                    'flow.telemetry.propagator.tracecontext',
                    new Definition(W3CTraceContext::class),
                );
                $builder->setDefinition('flow.telemetry.propagator.baggage', new Definition(W3CBaggage::class));

                $compositeDefinition = new Definition(CompositePropagator::class);
                $compositeDefinition->setArgument(0, [
                    new Reference('flow.telemetry.propagator.tracecontext'),
                    new Reference('flow.telemetry.propagator.baggage'),
                ]);
                $builder->setDefinition('flow.telemetry.propagator', $compositeDefinition);

                break;

            case 'tracecontext':
                $builder->setDefinition('flow.telemetry.propagator', new Definition(W3CTraceContext::class));

                break;

            case 'baggage':
                $builder->setDefinition('flow.telemetry.propagator', new Definition(W3CBaggage::class));

                break;

            default:
                throw new RuntimeException(sprintf('Unknown propagator type: %s', $type));
        }
    }

    /**
     * @param array{detectors?: array{enabled?: bool, static?: array{cache?: array{enabled?: bool, path?: null|string}, os?: array{enabled?: bool}, host?: array{enabled?: bool}, service?: array{enabled?: bool}, deployment?: array{enabled?: bool}, git?: array{enabled?: bool, binary?: string, working_directory?: null|string}, environment?: array{enabled?: bool}}, dynamic?: array{process?: array{enabled?: bool}}}, custom?: array<string, mixed>} $resourceConfig
     */
    private function registerResource(array $resourceConfig, ContainerBuilder $builder): void
    {
        $detectorsConfig = $resourceConfig['detectors'] ?? [];
        $detectorsEnabled = $detectorsConfig['enabled'] ?? true;

        if (!$detectorsEnabled) {
            $customAttributes = $resourceConfig['custom'] ?? [];
            $definition = new Definition(Resource::class);
            $definition->setFactory([Resource::class, 'create']);
            $definition->setArgument(0, $customAttributes);
            $builder->setDefinition('flow.telemetry.resource', $definition);

            return;
        }

        $staticConfig = $detectorsConfig['static'] ?? [];
        $dynamicConfig = $detectorsConfig['dynamic'] ?? [];
        $customAttributes = $resourceConfig['custom'] ?? [];

        $staticDetectorRefs = [];

        if ($staticConfig['os']['enabled'] ?? true) {
            $builder->setDefinition('flow.telemetry.resource.detector.os', new Definition(OsDetector::class));
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.os');
        }

        if ($staticConfig['host']['enabled'] ?? true) {
            $builder->setDefinition('flow.telemetry.resource.detector.host', new Definition(HostDetector::class));
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.host');
        }

        if ($staticConfig['service']['enabled'] ?? true) {
            $composerDefinition = new Definition(ComposerDetector::class);
            $composerDefinition->setArgument(0, '%kernel.project_dir%/composer.json');
            $builder->setDefinition('flow.telemetry.resource.detector.service', $composerDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.service');
        }

        if ($staticConfig['deployment']['enabled'] ?? true) {
            $deploymentDefinition = new Definition(SymfonyDeploymentDetector::class);
            $deploymentDefinition->setArgument(0, '%kernel.environment%');
            $builder->setDefinition('flow.telemetry.resource.detector.deployment', $deploymentDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.deployment');
        }

        if ($staticConfig['git']['enabled'] ?? false) {
            $gitConfig = is_array($staticConfig['git'] ?? null) ? $staticConfig['git'] : [];
            $workingDirectory = $gitConfig['working_directory'] ?? null;
            $gitBinary = is_string($gitConfig['binary'] ?? null) ? $gitConfig['binary'] : 'git';

            $gitDefinition = new Definition(GitDetector::class);
            $gitDefinition->setArgument(0, $workingDirectory ?? '%kernel.project_dir%');
            $gitDefinition->setArgument(1, $gitBinary);
            $builder->setDefinition('flow.telemetry.resource.detector.git', $gitDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.git');
        }

        if (count($customAttributes) > 0) {
            $manualDefinition = new Definition(ManualDetector::class);
            $manualDefinition->setArgument(0, $customAttributes);
            $builder->setDefinition('flow.telemetry.resource.detector.custom', $manualDefinition);
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.custom');
        }

        if ($staticConfig['environment']['enabled'] ?? true) {
            $builder->setDefinition(
                'flow.telemetry.resource.detector.environment',
                new Definition(EnvironmentDetector::class),
            );
            $staticDetectorRefs[] = new Reference('flow.telemetry.resource.detector.environment');
        }

        $staticChainDefinition = new Definition(ChainDetector::class);
        $staticChainDefinition->setArguments($staticDetectorRefs);
        $builder->setDefinition('flow.telemetry.resource.detector.static.chain', $staticChainDefinition);

        $cacheConfig = $staticConfig['cache'] ?? [];
        $cacheEnabled = $cacheConfig['enabled'] ?? true;

        if ($cacheEnabled) {
            $cachingDefinition = new Definition(CachingDetector::class);
            $cachingDefinition->setArgument(0, new Reference('flow.telemetry.resource.detector.static.chain'));
            $cachingDefinition->setArgument(
                1,
                $cacheConfig['path'] ?? sys_get_temp_dir() . '/flow_telemetry_resource_%kernel.environment%.cache',
            );
            $builder->setDefinition('flow.telemetry.resource.detector.static', $cachingDefinition);
        } else {
            $builder->setAlias(
                'flow.telemetry.resource.detector.static',
                'flow.telemetry.resource.detector.static.chain',
            );
        }

        $dynamicDetectorRefs = [];

        if ($dynamicConfig['process']['enabled'] ?? true) {
            $builder->setDefinition(
                'flow.telemetry.resource.detector.process',
                new Definition(ProcessDetector::class),
            );
            $dynamicDetectorRefs[] = new Reference('flow.telemetry.resource.detector.process');
        }

        if (count($dynamicDetectorRefs) > 0) {
            $dynamicChainDefinition = new Definition(ChainDetector::class);
            $dynamicChainDefinition->setArguments($dynamicDetectorRefs);
            $builder->setDefinition('flow.telemetry.resource.detector.dynamic', $dynamicChainDefinition);

            $finalChainDefinition = new Definition(ChainDetector::class);
            $finalChainDefinition->setArguments([
                new Reference('flow.telemetry.resource.detector.static'),
                new Reference('flow.telemetry.resource.detector.dynamic'),
            ]);
            $builder->setDefinition('flow.telemetry.resource.detector', $finalChainDefinition);
        } else {
            $builder->setAlias('flow.telemetry.resource.detector', 'flow.telemetry.resource.detector.static');
        }

        $resourceDefinition = new Definition(Resource::class);
        $resourceDefinition->setFactory([new Reference('flow.telemetry.resource.detector'), 'detect']);
        $builder->setDefinition('flow.telemetry.resource', $resourceDefinition);
    }

    /**
     * @param array<array-key, mixed> $config
     */
    private function registerTelemetry(array $config, ContainerBuilder $builder): void
    {
        $tracerProviderConfig = is_array($config['tracer_provider'] ?? null) ? $config['tracer_provider'] : [];
        $meterProviderConfig = is_array($config['meter_provider'] ?? null) ? $config['meter_provider'] : [];
        $loggerProviderConfig = is_array($config['logger_provider'] ?? null) ? $config['logger_provider'] : [];

        $tracerProviderServiceId = $this->buildTracerProvider($tracerProviderConfig, $builder);
        $meterProviderServiceId = $this->buildMeterProvider($meterProviderConfig, $builder);
        $loggerProviderServiceId = $this->buildLoggerProvider($loggerProviderConfig, $builder);

        $telemetryServiceId = 'flow.telemetry';
        $definition = new Definition(Telemetry::class);
        $definition->setArgument(0, new Reference('flow.telemetry.resource'));
        $definition->setArgument(1, new Reference($tracerProviderServiceId));
        $definition->setArgument(2, new Reference($meterProviderServiceId));
        $definition->setArgument(3, new Reference($loggerProviderServiceId));
        $definition->setPublic(true);
        $builder->setDefinition($telemetryServiceId, $definition);

        $builder->setAlias(Telemetry::class, $telemetryServiceId)->setPublic(true);
    }

    /**
     * @param array<string, array{version?: string, schema_url?: null|string, attributes?: array{scope?: array<string, mixed>, signal?: array<string, mixed>}}> $config
     */
    private function registerTracers(array $config, ContainerBuilder $builder): void
    {
        foreach ($config as $name => $tracerConfig) {
            $definition = new Definition(Tracer::class);
            $definition->setFactory([new Reference('flow.telemetry'), 'tracer']);
            $definition->setArgument(0, $name);
            $definition->setArgument(1, $tracerConfig['version'] ?? 'unknown');
            $definition->setArgument(2, $tracerConfig['schema_url'] ?? null);

            self::applyScopeAndSignalAttributes($definition, $tracerConfig);

            $definition->setPublic(true);
            $builder->setDefinition('flow.telemetry.' . $name . '.tracer', $definition);
        }
    }

    private function resolveErrorHandlerReference(mixed $name, ContainerBuilder $builder): Reference
    {
        if (!is_string($name) || $name === '') {
            $name = 'default';
        }

        $serviceId = 'flow.telemetry.error_handler.' . $name;

        if (!$builder->hasDefinition($serviceId) && !$builder->hasAlias($serviceId)) {
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
        ContainerBuilder $builder,
    ): Reference {
        if (!is_string($exporterName) || $exporterName === '') {
            throw new RuntimeException(sprintf(
                'Missing "exporter" reference for %s processor; expected a name from top-level "exporters"',
                $signalLabel,
            ));
        }

        $serviceId = 'flow.telemetry.exporter.' . $exporterName;

        if (!$builder->hasDefinition($serviceId) && !$builder->hasAlias($serviceId)) {
            throw new RuntimeException(sprintf(
                '%s processor references unknown exporter "%s"',
                ucfirst($signalLabel),
                $exporterName,
            ));
        }

        return new Reference($serviceId);
    }

    private function serviceExporterNode(): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder('service');
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

    private function transportNode(string $name = 'transport', bool $allowFailover = true): ArrayNodeDefinition
    {
        $builder = new \Symfony\Component\Config\Definition\Builder\TreeBuilder($name);
        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();

        $node->info(
            $allowFailover
                ? 'Transport configuration (required when exporter type is "otlp")'
                : 'Optional failover transport receiving prior batches when the primary transport fails (curl/grpc primaries only).',
        );
        $this->applyTransportSchema($node, allowFailover: $allowFailover);

        return $node;
    }
}
