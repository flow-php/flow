<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\CatalogProvider\ArrayCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\SessionPurgeCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler\CatalogProviderPass;
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler\CommandLocatorPass;
use Flow\Bridge\Symfony\PostgreSqlBundle\Generator\TwigMigrationGenerator;
use Flow\Bridge\Symfony\PostgreSqlBundle\Messenger\FlowPostgreSqlTransportFactory;
use Flow\Bridge\Symfony\PostgreSqlBundle\Repository\FilesystemMigrationRepository;
use Flow\Bridge\Symfony\PostgreSQLCache\CacheCatalogProvider;
use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;
use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\Bridge\Symfony\PostgreSQLSession\SessionCatalogProvider;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Context;
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryConfig;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryOptions;
use Flow\PostgreSql\Client\Telemetry\TraceableClient;
use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Flow\PostgreSql\Migrations\MigrationsFactory;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\VersionGenerator\TimestampVersionGenerator;
use Flow\PostgreSql\Migrations\VersionResolver;
use Flow\PostgreSql\Schema\Exclusion\AnyExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\EndsWithExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\ExactMatchExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\PatternExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\ScopedExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use Flow\PostgreSql\Schema\Exclusion\StartsWithExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\WholeSchemaExclusionPolicy;
use Flow\Telemetry\Provider\Clock\SystemClock;
use LogicException;
use Override;
use Reflector;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\Argument\ServiceClosureArgument;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;
use Twig\Environment;
use Twig\Loader\FilesystemLoader;

use function array_key_exists;
use function array_keys;
use function class_exists;
use function Flow\Types\DSL\type_string;
use function implode;
use function in_array;
use function sprintf;

final class FlowPostgreSqlBundle extends AbstractBundle
{
    protected string $extensionAlias = 'flow_postgresql';

    #[Override]
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);

        $container->addCompilerPass(new CatalogProviderPass());
        $container->addCompilerPass(new CommandLocatorPass());

        $container->registerAttributeForAutoconfiguration(AsCatalogProvider::class, static function (
            ChildDefinition $definition,
            AsCatalogProvider $attribute,
            Reflector $reflector,
        ): void {
            $definition->addTag('flow.postgresql.catalog_provider');
        });
    }

    #[Override]
    public function configure(DefinitionConfigurator $definition): void
    {
        $definition
            ->rootNode()
            ->children()
            ->arrayNode('connections')
            ->requiresAtLeastOneElement()
            ->useAttributeAsKey('name')
            ->arrayPrototype()
            ->children()
            ->scalarNode('dsn')
            ->isRequired()
            ->cannotBeEmpty()
            ->info('PostgreSQL connection DSN (e.g. postgresql://user:pass@localhost:5432/dbname)')
            ->end()
            ->booleanNode('test_transaction_rollback')
            ->defaultFalse()
            ->info(
                'When true and flow-php/phpunit-postgresql-bridge is installed, wraps the connection with StaticClient for transaction rollback in tests.',
            )
            ->end()
            ->arrayNode('context')
            ->info(
                'Extra key/value pairs merged into the Flow\\PostgreSql\\Client\\Context for every mapper call. Values can be literals, @service_id references, %parameter% placeholders, or %env(VAR)% expressions.',
            )
            ->useAttributeAsKey('name')
            ->variablePrototype()
            ->end()
            ->end()
            ->arrayNode('telemetry')
            ->children()
            ->scalarNode('service_id')
            ->isRequired()
            ->cannotBeEmpty()
            ->info('Service ID of the Telemetry instance (e.g. flow.telemetry)')
            ->end()
            ->scalarNode('clock_service_id')
            ->defaultNull()
            ->info('Service ID of a PSR ClockInterface implementation. Default: creates SystemClock')
            ->end()
            ->booleanNode('trace_queries')
            ->defaultTrue()
            ->end()
            ->booleanNode('trace_transactions')
            ->defaultTrue()
            ->end()
            ->booleanNode('collect_metrics')
            ->defaultTrue()
            ->end()
            ->booleanNode('log_queries')
            ->defaultFalse()
            ->end()
            ->integerNode('max_query_length')
            ->defaultValue(1000)
            ->min(0)
            ->end()
            ->booleanNode('include_parameters')
            ->defaultFalse()
            ->end()
            ->integerNode('max_parameters')
            ->defaultValue(10)
            ->min(0)
            ->end()
            ->integerNode('max_parameter_length')
            ->defaultValue(100)
            ->min(0)
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('messenger')
            ->info(
                'Enables the Symfony Messenger PostgreSQL transport. Requires flow-php/symfony-postgresql-messenger-bridge.',
            )
            ->canBeEnabled()
            ->children()
            ->scalarNode('table_name')
            ->defaultValue('messenger_messages')
            ->cannotBeEmpty()
            ->info('Name of the table that stores messenger messages.')
            ->end()
            ->scalarNode('schema')
            ->defaultValue('public')
            ->cannotBeEmpty()
            ->info('Schema that owns the messenger table.')
            ->end()
            ->end()
            ->end()
            ->arrayNode('cache')
            ->info('Defines PostgreSQL-backed Symfony Cache pools. Requires flow-php/symfony-postgresql-cache-bridge.')
            ->addDefaultsIfNotSet()
            ->children()
            ->arrayNode('pools')
            ->info(
                'Named cache pools. Each becomes a service "flow.postgresql.cache.pool.<name>" usable as adapter: <id> in framework.cache.pools.',
            )
            ->useAttributeAsKey('name')
            ->arrayPrototype()
            ->children()
            ->scalarNode('connection')
            ->defaultNull()
            ->info('flow_postgresql.connections key to use. Defaults to the first declared connection when null.')
            ->end()
            ->scalarNode('table_name')
            ->defaultValue('cache_items')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('schema')
            ->defaultValue('public')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('id_col')
            ->defaultValue('item_id')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('data_col')
            ->defaultValue('item_data')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('lifetime_col')
            ->defaultValue('item_lifetime')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('time_col')
            ->defaultValue('item_time')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('namespace')
            ->defaultValue('')
            ->info('Cache pool namespace. Allowed chars: -+.A-Za-z0-9')
            ->end()
            ->integerNode('default_lifetime')
            ->defaultValue(0)
            ->min(0)
            ->end()
            ->scalarNode('marshaller_service_id')
            ->defaultNull()
            ->end()
            ->booleanNode('share_connection')
            ->defaultFalse()
            ->info(
                'When true, the pool reuses the named connection\'s Client instead of opening its own pg_connect. Off by default.',
            )
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('session')
            ->info(
                'Defines the PostgreSQL-backed Symfony Session handler. Requires flow-php/symfony-postgresql-session-bridge.',
            )
            ->canBeEnabled()
            ->children()
            ->scalarNode('connection')
            ->defaultNull()
            ->info('flow_postgresql.connections key to use. Defaults to the first declared connection when null.')
            ->end()
            ->scalarNode('table_name')
            ->defaultValue('sessions')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('schema')
            ->defaultValue('public')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('id_col')
            ->defaultValue('sess_id')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('data_col')
            ->defaultValue('sess_data')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('lifetime_col')
            ->defaultValue('sess_lifetime')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('time_col')
            ->defaultValue('sess_time')
            ->cannotBeEmpty()
            ->end()
            ->enumNode('lock_mode')
            ->values(['none', 'advisory', 'transactional'])
            ->defaultValue('transactional')
            ->info(
                'Locking strategy. "transactional" uses SELECT FOR UPDATE; "advisory" uses pg_advisory_lock; "none" disables locking.',
            )
            ->end()
            ->integerNode('ttl')
            ->defaultNull()
            ->min(0)
            ->info('Session lifetime in seconds. When null, falls back to ini "session.gc_maxlifetime".')
            ->end()
            ->booleanNode('share_connection')
            ->defaultFalse()
            ->info(
                'When true, the handler reuses the named connection\'s Client instead of opening its own pg_connect. Off by default.',
            )
            ->end()
            ->end()
            ->end()
            ->arrayNode('migrations')
            ->canBeEnabled()
            ->children()
            ->scalarNode('directory')
            ->defaultValue('%kernel.project_dir%/migrations')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('namespace')
            ->defaultValue('App\\Migrations')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('table_name')
            ->defaultValue('flow_migrations')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('table_schema')
            ->defaultValue('public')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('migration_file_name')
            ->defaultValue('migration.php')
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('rollback_file_name')
            ->defaultValue('rollback.php')
            ->cannotBeEmpty()
            ->end()
            ->booleanNode('all_or_nothing')
            ->defaultFalse()
            ->info('Wrap all migrations in a single transaction (default: false)')
            ->end()
            ->booleanNode('generate_rollback')
            ->defaultTrue()
            ->info('Generate rollback files when creating migrations (default: true)')
            ->end()
            ->arrayNode('exclude')
            ->info('Schema objects excluded from migration diffing (e.g. tables created dynamically at runtime). Each entry defines exactly one matcher: schema, table, exact, starts_with, ends_with, pattern or policy_id.')
            ->arrayPrototype()
            ->children()
            ->scalarNode('schema')
            ->defaultNull()
            ->info('Exclude an entire schema with all of its objects.')
            ->end()
            ->scalarNode('table')
            ->defaultNull()
            ->info('Exclude a table by exact name (shorthand for exact scoped to tables).')
            ->end()
            ->scalarNode('exact')
            ->defaultNull()
            ->info('Exclude any object whose name matches exactly.')
            ->end()
            ->scalarNode('starts_with')
            ->defaultNull()
            ->info('Exclude objects whose name starts with this prefix.')
            ->end()
            ->scalarNode('ends_with')
            ->defaultNull()
            ->info('Exclude objects whose name ends with this suffix.')
            ->end()
            ->scalarNode('pattern')
            ->defaultNull()
            ->info('Exclude objects whose name matches this PCRE pattern (with delimiters).')
            ->end()
            ->scalarNode('policy_id')
            ->defaultNull()
            ->info('Service ID of a custom Flow\\PostgreSql\\Schema\\Exclusion\\ExclusionPolicy.')
            ->end()
            ->enumNode('type')
            ->values(['table', 'view', 'materialized_view', 'sequence', 'function', 'procedure', 'domain', 'extension'])
            ->defaultNull()
            ->info('Narrow the matcher to a single object type (ignored when using "schema" or "table").')
            ->end()
            ->scalarNode('for_schema')
            ->defaultNull()
            ->info('Narrow the matcher to a single schema.')
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('catalog_providers')
            ->info(
                'List of catalog providers to merge into the target schema. Each entry must have either "catalog_provider_id" or "catalog".',
            )
            ->arrayPrototype()
            ->children()
            ->scalarNode('catalog_provider_id')
            ->defaultNull()
            ->info('Service ID of Flow\\PostgreSql\\Schema\\CatalogProvider')
            ->end()
            ->variableNode('catalog')
            ->defaultNull()
            ->info('Inline catalog definition matching Catalog::fromArray() shape')
            ->end()
            ->end()
            ->end()
            ->end()
            ->end();
    }

    /**
     * @param array{connections: array<string, array{dsn: string, test_transaction_rollback: bool, context?: array<string, mixed>, telemetry?: array{service_id: string, clock_service_id: ?string, trace_queries: bool, trace_transactions: bool, collect_metrics: bool, log_queries: bool, max_query_length: int, include_parameters: bool, max_parameters: int, max_parameter_length: int}}>, messenger: array{enabled: bool, table_name: string, schema: string}, cache: array{pools?: array<string, array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string, share_connection: bool}>}, session: array{enabled: bool, connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, lock_mode: string, ttl: ?int, share_connection: bool}, migrations: array{enabled: bool, directory: string, namespace: string, table_name: string, table_schema: string, migration_file_name: string, rollback_file_name: string, all_or_nothing: bool, generate_rollback: bool, exclude?: list<array{schema: ?string, table: ?string, exact: ?string, starts_with: ?string, ends_with: ?string, pattern: ?string, policy_id: ?string, type: ?string, for_schema: ?string}>}, catalog_providers: list<array{catalog_provider_id: ?string, catalog: ?array<string, mixed>}>} $config
     */
    #[Override]
    public function loadExtension(array $config, ContainerConfigurator $configurator, ContainerBuilder $container): void
    {
        $isFirst = true;
        $connectionNames = array_keys($config['connections']);

        foreach ($config['connections'] as $name => $connectionConfig) {
            $this->registerConnection($name, $connectionConfig, $container, $isFirst);
            $isFirst = false;
        }

        $this->registerCatalogProviders($config['catalog_providers'] ?? [], $container);

        $container->setParameter('flow.postgresql.connections', $connectionNames);
        $container->setParameter('flow.postgresql.default_connection', $connectionNames[0]);

        $configurator->import(__DIR__ . '/Resources/config/database.php');
        $configurator->import(__DIR__ . '/Resources/config/format.php');

        if ($config['migrations']['enabled']) {
            $isFirst = true;

            foreach ($connectionNames as $name) {
                $this->registerMigrations($name, $config['migrations'], $container, $isFirst);
                $isFirst = false;
            }

            $container->setParameter('flow.postgresql.migrations.connections', $connectionNames);
            $container->setParameter('flow.postgresql.migrations.default_connection', $connectionNames[0]);

            $configurator->import(__DIR__ . '/Resources/config/migrations.php');
        }

        $this->registerMessenger($config['messenger'], $connectionNames, $container);
        $this->registerCache($config['cache'] ?? [], $connectionNames, $container);
        $this->registerSession($config['session'] ?? [], $connectionNames, $container);
    }

    /**
     * @param array{pools?: array<string, array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string, share_connection: bool}>} $cacheConfig
     * @param list<string> $connectionNames
     */
    private function registerCache(array $cacheConfig, array $connectionNames, ContainerBuilder $container): void
    {
        if (!class_exists(FlowPostgreSqlCacheAdapter::class)) {
            return;
        }

        $pools = $cacheConfig['pools'] ?? [];

        if ($pools === []) {
            return;
        }

        foreach ($pools as $name => $poolConfig) {
            $this->registerCachePool((string) $name, $poolConfig, $connectionNames, $container);
        }
    }

    /**
     * @param array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string, share_connection: bool} $poolConfig
     * @param list<string> $connectionNames
     */
    private function registerCachePool(
        string $name,
        array $poolConfig,
        array $connectionNames,
        ContainerBuilder $container,
    ): void {
        $connectionName = $poolConfig['connection'] ?? $connectionNames[0];

        if (!in_array($connectionName, $connectionNames, true)) {
            throw new LogicException(sprintf(
                'Cache pool "%s" references unknown connection "%s". Declared connections: %s',
                $name,
                $connectionName,
                implode(', ', $connectionNames),
            ));
        }

        $catalogDef = new Definition(CacheCatalogProvider::class, [
            $poolConfig['table_name'],
            $poolConfig['schema'],
            $poolConfig['id_col'],
            $poolConfig['data_col'],
            $poolConfig['lifetime_col'],
            $poolConfig['time_col'],
        ]);
        $catalogDef->addTag('flow.postgresql.catalog_provider');
        $container->setDefinition("flow.postgresql.cache.pool.{$name}.catalog_provider", $catalogDef);

        $connectionRef = $poolConfig['share_connection'] ?? false
            ? new Reference("flow.postgresql.{$connectionName}.client")
            : new Reference("flow.postgresql.{$connectionName}.connection_parameters");

        $adapterDef = new Definition(FlowPostgreSqlCacheAdapter::class, [
            $connectionRef,
            $poolConfig['namespace'],
            $poolConfig['default_lifetime'],
            [
                'db_table' => $poolConfig['table_name'],
                'db_schema' => $poolConfig['schema'],
                'db_id_col' => $poolConfig['id_col'],
                'db_data_col' => $poolConfig['data_col'],
                'db_lifetime_col' => $poolConfig['lifetime_col'],
                'db_time_col' => $poolConfig['time_col'],
            ],
            $poolConfig['marshaller_service_id'] !== null ? new Reference($poolConfig['marshaller_service_id']) : null,
        ]);
        $adapterDef->setPublic(true);
        $container->setDefinition("flow.postgresql.cache.pool.{$name}", $adapterDef);
    }

    /**
     * @param list<array{catalog_provider_id: ?string, catalog: ?array<string, mixed>}> $catalogProviders
     */
    private function registerCatalogProviders(array $catalogProviders, ContainerBuilder $container): void
    {
        $configProviderServiceIds = [];

        foreach ($catalogProviders as $i => $providerConfig) {
            if (array_key_exists('catalog', $providerConfig) && $providerConfig['catalog'] !== null) {
                $providerDef = new Definition(ArrayCatalogProvider::class, [$providerConfig['catalog']]);
                $providerDef->addTag('flow.postgresql.catalog_provider');
                $container->setDefinition("flow.postgresql.catalog_provider.{$i}", $providerDef);
            } elseif ($providerConfig['catalog_provider_id'] !== null) {
                $configProviderServiceIds[] = type_string()->assert($providerConfig['catalog_provider_id']);
            }
        }

        if ($configProviderServiceIds !== []) {
            $container->setParameter('flow.postgresql.catalog_provider.service_ids', $configProviderServiceIds);
        }
    }

    /**
     * @param array{dsn: string, test_transaction_rollback: bool, context?: array<string, mixed>, telemetry?: array{service_id: string, clock_service_id: ?string, trace_queries: bool, trace_transactions: bool, collect_metrics: bool, log_queries: bool, max_query_length: int, include_parameters: bool, max_parameters: int, max_parameter_length: int}} $connectionConfig
     */
    private function registerConnection(
        string $name,
        array $connectionConfig,
        ContainerBuilder $container,
        bool $isFirst,
    ): void {
        $parserDef = new Definition(DsnParser::class);
        $container->setDefinition("flow.postgresql.{$name}.dsn_parser", $parserDef);

        $paramsDef = new Definition(ConnectionParameters::class);
        $paramsDef->setFactory([new Reference("flow.postgresql.{$name}.dsn_parser"), 'parse']);
        $paramsDef->setArguments([$connectionConfig['dsn']]);
        $container->setDefinition("flow.postgresql.{$name}.connection_parameters", $paramsDef);

        $paramsDef->setPublic(true);

        $clientArguments = [new Reference("flow.postgresql.{$name}.connection_parameters")];

        if (array_key_exists('context', $connectionConfig) && $connectionConfig['context'] !== []) {
            $contextDef = new Definition(Context::class, [
                null,
                $connectionConfig['context'],
            ]);
            $container->setDefinition("flow.postgresql.{$name}.context", $contextDef);

            $clientArguments[] = null;
            $clientArguments[] = new Reference("flow.postgresql.{$name}.context");
        }

        $clientDef = new Definition(PgSqlClient::class);
        $clientDef->setFactory([PgSqlClient::class, 'connect']);
        $clientDef->setArguments($clientArguments);
        $clientDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.client", $clientDef);

        if ($connectionConfig['test_transaction_rollback']) {
            $this->registerStaticConnection($name, $container);
        }

        if (array_key_exists('telemetry', $connectionConfig)) {
            $this->registerTelemetry($name, $connectionConfig['telemetry'], $container);
        }

        if ($isFirst) {
            $container->setAlias(Client::class, "flow.postgresql.{$name}.client");
            $container->setAlias(ConnectionParameters::class, "flow.postgresql.{$name}.connection_parameters");
        }
    }

    /**
     * @param array{enabled: bool, table_name: string, schema: string} $messengerConfig
     * @param list<string> $connectionNames
     */
    private function registerMessenger(
        array $messengerConfig,
        array $connectionNames,
        ContainerBuilder $container,
    ): void {
        if (!class_exists(FlowPostgreSqlTransportFactory::class)) {
            return;
        }

        if (!$messengerConfig['enabled']) {
            return;
        }

        $catalogProviderDef = new Definition(MessengerCatalogProvider::class, [
            $messengerConfig['table_name'],
            $messengerConfig['schema'],
        ]);
        $catalogProviderDef->addTag('flow.postgresql.catalog_provider');
        $container->setDefinition('flow.postgresql.messenger.catalog_provider', $catalogProviderDef);

        $locatorServices = [];

        foreach ($connectionNames as $name) {
            $locatorServices[$name] = new ServiceClosureArgument(new Reference("flow.postgresql.{$name}.client"));
        }

        $locatorDef = new Definition(ServiceLocator::class, [$locatorServices]);
        $locatorDef->addTag('container.service_locator');
        $container->setDefinition('flow.postgresql.messenger.client_locator', $locatorDef);

        $factoryDef = new Definition(FlowPostgreSqlTransportFactory::class, [
            new Reference('flow.postgresql.messenger.client_locator'),
        ]);
        $factoryDef->addTag('messenger.transport_factory');
        $container->setDefinition('flow.postgresql.messenger.transport_factory', $factoryDef);
    }

    /**
     * @param array{enabled: bool, directory: string, namespace: string, table_name: string, table_schema: string, migration_file_name: string, rollback_file_name: string, all_or_nothing: bool, generate_rollback: bool, exclude?: list<array{schema: ?string, table: ?string, exact: ?string, starts_with: ?string, ends_with: ?string, pattern: ?string, policy_id: ?string, type: ?string, for_schema: ?string}>} $mc
     */
    private function registerMigrations(string $name, array $mc, ContainerBuilder $container, bool $isFirst): void
    {
        $catalogProviderRef = new Reference('flow.postgresql.catalog_provider');

        $configDef = new Definition(MigrationsConfiguration::class, [
            new Reference("flow.postgresql.{$name}.client"),
            $catalogProviderRef,
            $mc['directory'],
            $mc['namespace'],
            $mc['table_name'],
            $mc['table_schema'],
            $mc['migration_file_name'],
            $mc['rollback_file_name'],
            $mc['all_or_nothing'],
            $mc['generate_rollback'],
            $this->buildExclusionPolicy($mc['exclude'] ?? []),
        ]);
        $configDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.configuration", $configDef);

        $fsDef = new Definition(NativeLocalFilesystem::class);
        $container->setDefinition("flow.postgresql.{$name}.migrations.filesystem", $fsDef);

        $pathDef = new Definition(Path::class);
        $pathDef->setFactory([Path::class, 'from']);
        $pathDef->setArguments([$mc['directory']]);
        $container->setDefinition("flow.postgresql.{$name}.migrations.path", $pathDef);

        $repoDef = new Definition(FilesystemMigrationRepository::class, [
            new Reference("flow.postgresql.{$name}.migrations.filesystem"),
            new Reference("flow.postgresql.{$name}.migrations.path"),
            new Reference("flow.postgresql.{$name}.migrations.configuration"),
        ]);
        $repoDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.repository", $repoDef);

        $factoryDef = new Definition(MigrationsFactory::class, [
            new Reference("flow.postgresql.{$name}.migrations.configuration"),
            new Reference("flow.postgresql.{$name}.migrations.repository"),
        ]);
        $container->setDefinition("flow.postgresql.{$name}.migrations.factory", $factoryDef);

        $migratorDef = new Definition(Migrator::class);
        $migratorDef->setFactory([new Reference("flow.postgresql.{$name}.migrations.factory"), 'createMigrator']);
        $migratorDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.migrator", $migratorDef);

        $storeDef = new Definition(MigrationStore::class);
        $storeDef->setFactory([new Reference("flow.postgresql.{$name}.migrations.factory"), 'createStore']);
        $storeDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.store", $storeDef);

        $executorDef = new Definition(MigrationExecutor::class);
        $executorDef->setFactory([new Reference("flow.postgresql.{$name}.migrations.factory"), 'createExecutor']);
        $container->setDefinition("flow.postgresql.{$name}.migrations.executor", $executorDef);

        $resolverDef = new Definition(VersionResolver::class);
        $resolverDef->setFactory([
            new Reference("flow.postgresql.{$name}.migrations.factory"),
            'createVersionResolver',
        ]);
        $resolverDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.version_resolver", $resolverDef);

        $twigLoaderDef = new Definition(FilesystemLoader::class, [
            [__DIR__ . '/Resources/templates'],
        ]);
        $container->setDefinition("flow.postgresql.{$name}.migrations.twig_loader", $twigLoaderDef);

        $twigDef = new Definition(Environment::class, [
            new Reference("flow.postgresql.{$name}.migrations.twig_loader"),
        ]);
        $container->setDefinition("flow.postgresql.{$name}.migrations.twig", $twigDef);

        $versionGenDef = new Definition(TimestampVersionGenerator::class);
        $container->setDefinition("flow.postgresql.{$name}.migrations.version_generator", $versionGenDef);

        $generatorDef = new Definition(TwigMigrationGenerator::class, [
            new Reference("flow.postgresql.{$name}.migrations.configuration"),
            new Reference("flow.postgresql.{$name}.migrations.version_generator"),
            new Reference("flow.postgresql.{$name}.migrations.twig"),
            new Reference("flow.postgresql.{$name}.migrations.filesystem"),
        ]);
        $generatorDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.generator", $generatorDef);

        $diffGenDef = new Definition(DiffMigrationGenerator::class);
        $diffGenDef->setFactory([new Reference("flow.postgresql.{$name}.migrations.factory"), 'createDiffGenerator']);
        $diffGenDef->setArguments([new Reference("flow.postgresql.{$name}.migrations.generator")]);
        $diffGenDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.diff_generator", $diffGenDef);

        if ($isFirst) {
            $container->setAlias(MigrationsConfiguration::class, "flow.postgresql.{$name}.migrations.configuration");
            $container->setAlias(MigrationsFactory::class, "flow.postgresql.{$name}.migrations.factory");
            $container->setAlias(Migrator::class, "flow.postgresql.{$name}.migrations.migrator");
            $container->setAlias(MigrationStore::class, "flow.postgresql.{$name}.migrations.store");
            $container->setAlias(MigrationRepository::class, "flow.postgresql.{$name}.migrations.repository");
            $container->setAlias(MigrationExecutor::class, "flow.postgresql.{$name}.migrations.executor");
            $container->setAlias(VersionResolver::class, "flow.postgresql.{$name}.migrations.version_resolver");
            $container->setAlias(MigrationGenerator::class, "flow.postgresql.{$name}.migrations.generator");
            $container->setAlias(DiffMigrationGenerator::class, "flow.postgresql.{$name}.migrations.diff_generator");
        }
    }

    /**
     * @param list<array{schema: ?string, table: ?string, exact: ?string, starts_with: ?string, ends_with: ?string, pattern: ?string, policy_id: ?string, type: ?string, for_schema: ?string}> $exclude
     */
    private function buildExclusionPolicy(array $exclude): ?Definition
    {
        if ($exclude === []) {
            return null;
        }

        $policies = [];

        foreach ($exclude as $entry) {
            $policies[] = $this->buildExclusionPolicyEntry($entry);
        }

        return new Definition(AnyExclusionPolicy::class, $policies);
    }

    /**
     * @param array{schema: ?string, table: ?string, exact: ?string, starts_with: ?string, ends_with: ?string, pattern: ?string, policy_id: ?string, type: ?string, for_schema: ?string} $entry
     */
    private function buildExclusionPolicyEntry(array $entry): Definition|Reference
    {
        if ($entry['schema'] !== null) {
            return new Definition(WholeSchemaExclusionPolicy::class, [$entry['schema']]);
        }

        $impliedType = null;

        if ($entry['table'] !== null) {
            $matcher = new Definition(ExactMatchExclusionPolicy::class, [$entry['table']]);
            $impliedType = SchemaObjectType::TABLE;
        } elseif ($entry['exact'] !== null) {
            $matcher = new Definition(ExactMatchExclusionPolicy::class, [$entry['exact']]);
        } elseif ($entry['starts_with'] !== null) {
            $matcher = new Definition(StartsWithExclusionPolicy::class, [$entry['starts_with']]);
        } elseif ($entry['ends_with'] !== null) {
            $matcher = new Definition(EndsWithExclusionPolicy::class, [$entry['ends_with']]);
        } elseif ($entry['pattern'] !== null) {
            $matcher = new Definition(PatternExclusionPolicy::class, [$entry['pattern']]);
        } elseif ($entry['policy_id'] !== null) {
            $matcher = new Reference($entry['policy_id']);
        } else {
            throw new LogicException('Each "flow_postgresql.migrations.exclude" entry must define exactly one of: schema, table, exact, starts_with, ends_with, pattern, policy_id.');
        }

        $type = $entry['type'] !== null ? SchemaObjectType::from($entry['type']) : $impliedType;

        if ($type === null && $entry['for_schema'] === null) {
            return $matcher;
        }

        return new Definition(ScopedExclusionPolicy::class, [$matcher, $type, $entry['for_schema']]);
    }

    /**
     * @param array{enabled?: bool, connection?: ?string, table_name?: string, schema?: string, id_col?: string, data_col?: string, lifetime_col?: string, time_col?: string, lock_mode?: string, ttl?: ?int, share_connection?: bool} $sessionConfig
     * @param list<string> $connectionNames
     */
    private function registerSession(array $sessionConfig, array $connectionNames, ContainerBuilder $container): void
    {
        if (!class_exists(FlowPostgreSqlSessionHandler::class)) {
            return;
        }

        if (!($sessionConfig['enabled'] ?? false)) {
            return;
        }

        $connectionName = $sessionConfig['connection'] ?? $connectionNames[0];

        if (!in_array($connectionName, $connectionNames, true)) {
            throw new LogicException(sprintf(
                'Session references unknown connection "%s". Declared connections: %s',
                $connectionName,
                implode(', ', $connectionNames),
            ));
        }

        $tableName = $sessionConfig['table_name'] ?? 'sessions';
        $schema = $sessionConfig['schema'] ?? 'public';
        $idCol = $sessionConfig['id_col'] ?? 'sess_id';
        $dataCol = $sessionConfig['data_col'] ?? 'sess_data';
        $lifetimeCol = $sessionConfig['lifetime_col'] ?? 'sess_lifetime';
        $timeCol = $sessionConfig['time_col'] ?? 'sess_time';

        $catalogDef = new Definition(SessionCatalogProvider::class, [
            $tableName,
            $schema,
            $idCol,
            $dataCol,
            $lifetimeCol,
            $timeCol,
        ]);
        $catalogDef->addTag('flow.postgresql.catalog_provider');
        $container->setDefinition('flow.postgresql.session.catalog_provider', $catalogDef);

        $lockMode = match ($sessionConfig['lock_mode'] ?? 'transactional') {
            'none' => FlowPostgreSqlSessionHandler::LOCK_NONE,
            'advisory' => FlowPostgreSqlSessionHandler::LOCK_ADVISORY,
            default => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL,
        };

        $connectionRef = $sessionConfig['share_connection'] ?? false
            ? new Reference("flow.postgresql.{$connectionName}.client")
            : new Reference("flow.postgresql.{$connectionName}.connection_parameters");

        $handlerDef = new Definition(FlowPostgreSqlSessionHandler::class, [
            $connectionRef,
            [
                'db_table' => $tableName,
                'db_schema' => $schema,
                'db_id_col' => $idCol,
                'db_data_col' => $dataCol,
                'db_lifetime_col' => $lifetimeCol,
                'db_time_col' => $timeCol,
                'lock_mode' => $lockMode,
                'ttl' => $sessionConfig['ttl'] ?? null,
            ],
        ]);
        $handlerDef->setPublic(true);
        $container->setDefinition('flow.postgresql.session.handler', $handlerDef);

        $commandDef = new Definition(SessionPurgeCommand::class, [
            new Reference('flow.postgresql.session.handler'),
        ]);
        $commandDef->addTag('console.command');
        $container->setDefinition('flow.postgresql.session.purge_command', $commandDef);
    }

    private function registerStaticConnection(string $name, ContainerBuilder $container): void
    {
        if (!class_exists(StaticClient::class)) {
            throw new LogicException(sprintf(
                'Connection "%s" has test_transaction_rollback set to true, but flow-php/phpunit-postgresql-bridge is not installed. Run "composer require --dev flow-php/phpunit-postgresql-bridge".',
                $name,
            ));
        }

        $container->getDefinition("flow.postgresql.{$name}.client")->setFactory([StaticClient::class, 'connect']);
    }

    /**
     * @param array{service_id: string, clock_service_id: ?string, trace_queries: bool, trace_transactions: bool, collect_metrics: bool, log_queries: bool, max_query_length: int, include_parameters: bool, max_parameters: int, max_parameter_length: int} $telemetryConfig
     */
    private function registerTelemetry(string $name, array $telemetryConfig, ContainerBuilder $container): void
    {
        $optionsDef = new Definition(PostgreSqlTelemetryOptions::class, [
            $telemetryConfig['trace_queries'],
            $telemetryConfig['trace_transactions'],
            $telemetryConfig['collect_metrics'],
            $telemetryConfig['log_queries'],
            $telemetryConfig['max_query_length'],
            $telemetryConfig['include_parameters'],
            $telemetryConfig['max_parameters'],
            $telemetryConfig['max_parameter_length'],
        ]);
        $container->setDefinition("flow.postgresql.{$name}.telemetry.options", $optionsDef);

        if ($telemetryConfig['clock_service_id'] !== null) {
            $clockRef = new Reference($telemetryConfig['clock_service_id']);
        } else {
            $container->setDefinition("flow.postgresql.{$name}.telemetry.clock", new Definition(SystemClock::class));
            $clockRef = new Reference("flow.postgresql.{$name}.telemetry.clock");
        }

        $configDef = new Definition(PostgreSqlTelemetryConfig::class, [
            new Reference($telemetryConfig['service_id']),
            $clockRef,
            new Reference("flow.postgresql.{$name}.telemetry.options"),
        ]);
        $container->setDefinition("flow.postgresql.{$name}.telemetry.config", $configDef);

        $traceableDef = new Definition(TraceableClient::class, [
            new Reference("flow.postgresql.{$name}.client.telemetry.inner"),
            new Reference("flow.postgresql.{$name}.telemetry.config"),
        ]);
        $traceableDef->setDecoratedService("flow.postgresql.{$name}.client");
        $traceableDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.client.telemetry", $traceableDef);
    }
}
