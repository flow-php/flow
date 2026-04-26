<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection;

use function Flow\Types\DSL\type_string;

use Flow\Bridge\PHPUnit\PostgreSQL\StaticClient;
use Flow\Bridge\Symfony\PostgreSqlBundle\CatalogProvider\ArrayCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\SessionPurgeCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Generator\TwigMigrationGenerator;
use Flow\Bridge\Symfony\PostgreSqlBundle\Messenger\FlowPostgreSqlTransportFactory;
use Flow\Bridge\Symfony\PostgreSqlBundle\Repository\FilesystemMigrationRepository;
use Flow\Bridge\Symfony\PostgreSQLCache\{CacheCatalogProvider, FlowPostgreSqlCacheAdapter};
use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\Bridge\Symfony\PostgreSQLSession\{FlowPostgreSqlSessionHandler, SessionCatalogProvider};
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\PostgreSql\Client\{Client, ConnectionParameters, Context, DsnParser};
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Client\Telemetry\{PostgreSqlTelemetryConfig, PostgreSqlTelemetryOptions, TraceableClient};
use Flow\PostgreSql\Migrations\{Configuration as MigrationsConfiguration, MigrationsFactory, Migrator, VersionResolver};
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\{DiffMigrationGenerator, MigrationGenerator};
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\VersionGenerator\TimestampVersionGenerator;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Argument\ServiceClosureArgument;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference, ServiceLocator};
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Loader\PhpFileLoader;
use Twig\Environment;
use Twig\Loader\FilesystemLoader;

final class FlowPostgreSqlExtension extends Extension
{
    #[\Override]
    public function getAlias() : string
    {
        return 'flow_postgresql';
    }

    /**
     * @param array<array-key, mixed> $configs
     */
    public function load(array $configs, ContainerBuilder $container) : void
    {
        $configuration = new Configuration();

        /** @var array{connections: array<string, array{dsn: string, test_transaction_rollback: bool, context?: array<string, mixed>, telemetry?: array{service_id: string, clock_service_id: ?string, trace_queries: bool, trace_transactions: bool, collect_metrics: bool, log_queries: bool, max_query_length: int, include_parameters: bool, max_parameters: int, max_parameter_length: int}}>, messenger: array{enabled: bool, table_name: string, schema: string}, cache: array{pools?: array<string, array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string}>}, session: array{enabled: bool, connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, lock_mode: string, ttl: ?int}, migrations: array{enabled: bool, directory: string, namespace: string, table_name: string, table_schema: string, migration_file_name: string, rollback_file_name: string, all_or_nothing: bool, generate_rollback: bool}, catalog_providers: list<array{catalog_provider_id: ?string, catalog: ?array<string, mixed>}>} $config */
        $config = $this->processConfiguration($configuration, $configs);

        $isFirst = true;
        $connectionNames = \array_keys($config['connections']);

        foreach ($config['connections'] as $name => $connectionConfig) {
            $this->registerConnection($name, $connectionConfig, $container, $isFirst);
            $isFirst = false;
        }

        $this->registerCatalogProviders($config['catalog_providers'] ?? [], $container);

        $container->setParameter('flow.postgresql.connections', $connectionNames);
        $container->setParameter('flow.postgresql.default_connection', $connectionNames[0]);

        $loader = new PhpFileLoader($container, new FileLocator(__DIR__ . '/../Resources/config'));
        $loader->load('database.php');
        $loader->load('format.php');

        if ($config['migrations']['enabled']) {
            $isFirst = true;

            foreach ($connectionNames as $name) {
                $this->registerMigrations($name, $config['migrations'], $container, $isFirst);
                $isFirst = false;
            }

            $container->setParameter('flow.postgresql.migrations.connections', $connectionNames);
            $container->setParameter('flow.postgresql.migrations.default_connection', $connectionNames[0]);

            $loader->load('migrations.php');
        }

        $this->registerMessenger($config['messenger'], $connectionNames, $container);
        $this->registerCache($config['cache'] ?? [], $connectionNames, $container);
        $this->registerSession($config['session'] ?? [], $connectionNames, $container);
    }

    /**
     * @param array{pools?: array<string, array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string}>} $cacheConfig
     * @param list<string> $connectionNames
     */
    private function registerCache(array $cacheConfig, array $connectionNames, ContainerBuilder $container) : void
    {
        if (!\class_exists(FlowPostgreSqlCacheAdapter::class)) {
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
     * @param array{connection: ?string, table_name: string, schema: string, id_col: string, data_col: string, lifetime_col: string, time_col: string, namespace: string, default_lifetime: int, marshaller_service_id: ?string} $poolConfig
     * @param list<string> $connectionNames
     */
    private function registerCachePool(string $name, array $poolConfig, array $connectionNames, ContainerBuilder $container) : void
    {
        $connectionName = $poolConfig['connection'] ?? $connectionNames[0];

        if (!\in_array($connectionName, $connectionNames, true)) {
            throw new \LogicException(\sprintf(
                'Cache pool "%s" references unknown connection "%s". Declared connections: %s',
                $name,
                $connectionName,
                \implode(', ', $connectionNames),
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

        $adapterDef = new Definition(FlowPostgreSqlCacheAdapter::class, [
            new Reference("flow.postgresql.{$connectionName}.client"),
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
            $poolConfig['marshaller_service_id'] !== null
                ? new Reference($poolConfig['marshaller_service_id'])
                : null,
        ]);
        $adapterDef->setPublic(true);
        $container->setDefinition("flow.postgresql.cache.pool.{$name}", $adapterDef);
    }

    /**
     * @param list<array{catalog_provider_id: ?string, catalog: ?array<string, mixed>}> $catalogProviders
     */
    private function registerCatalogProviders(array $catalogProviders, ContainerBuilder $container) : void
    {
        $configProviderServiceIds = [];

        foreach ($catalogProviders as $i => $providerConfig) {
            if (\array_key_exists('catalog', $providerConfig) && $providerConfig['catalog'] !== null) {
                $providerDef = new Definition(ArrayCatalogProvider::class, [$providerConfig['catalog']]);
                $providerDef->addTag('flow.postgresql.catalog_provider');
                $container->setDefinition("flow.postgresql.catalog_provider.{$i}", $providerDef);
            } elseif (\array_key_exists('catalog_provider_id', $providerConfig) && $providerConfig['catalog_provider_id'] !== null) {
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
    private function registerConnection(string $name, array $connectionConfig, ContainerBuilder $container, bool $isFirst) : void
    {
        $parserDef = new Definition(DsnParser::class);
        $container->setDefinition("flow.postgresql.{$name}.dsn_parser", $parserDef);

        $paramsDef = new Definition(ConnectionParameters::class);
        $paramsDef->setFactory([new Reference("flow.postgresql.{$name}.dsn_parser"), 'parse']);
        $paramsDef->setArguments([$connectionConfig['dsn']]);
        $container->setDefinition("flow.postgresql.{$name}.connection_parameters", $paramsDef);

        $paramsDef->setPublic(true);

        $clientArguments = [new Reference("flow.postgresql.{$name}.connection_parameters")];

        if (\array_key_exists('context', $connectionConfig) && $connectionConfig['context'] !== []) {
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

        if (\array_key_exists('telemetry', $connectionConfig)) {
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
    private function registerMessenger(array $messengerConfig, array $connectionNames, ContainerBuilder $container) : void
    {
        if (!\class_exists(FlowPostgreSqlTransportFactory::class)) {
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
     * @param array{enabled: bool, directory: string, namespace: string, table_name: string, table_schema: string, migration_file_name: string, rollback_file_name: string, all_or_nothing: bool, generate_rollback: bool} $mc
     */
    private function registerMigrations(string $name, array $mc, ContainerBuilder $container, bool $isFirst) : void
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
        $resolverDef->setFactory([new Reference("flow.postgresql.{$name}.migrations.factory"), 'createVersionResolver']);
        $resolverDef->setPublic(true);
        $container->setDefinition("flow.postgresql.{$name}.migrations.version_resolver", $resolverDef);

        $twigLoaderDef = new Definition(FilesystemLoader::class, [
            [__DIR__ . '/../Resources/templates'],
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
     * @param array{enabled?: bool, connection?: ?string, table_name?: string, schema?: string, id_col?: string, data_col?: string, lifetime_col?: string, time_col?: string, lock_mode?: string, ttl?: ?int} $sessionConfig
     * @param list<string> $connectionNames
     */
    private function registerSession(array $sessionConfig, array $connectionNames, ContainerBuilder $container) : void
    {
        if (!\class_exists(FlowPostgreSqlSessionHandler::class)) {
            return;
        }

        if (!($sessionConfig['enabled'] ?? false)) {
            return;
        }

        $connectionName = $sessionConfig['connection'] ?? $connectionNames[0];

        if (!\in_array($connectionName, $connectionNames, true)) {
            throw new \LogicException(\sprintf(
                'Session references unknown connection "%s". Declared connections: %s',
                $connectionName,
                \implode(', ', $connectionNames),
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

        $handlerDef = new Definition(FlowPostgreSqlSessionHandler::class, [
            new Reference("flow.postgresql.{$connectionName}.client"),
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

    private function registerStaticConnection(string $name, ContainerBuilder $container) : void
    {
        if (!\class_exists(StaticClient::class)) {
            throw new \LogicException(\sprintf(
                'Connection "%s" has test_transaction_rollback set to true, but flow-php/phpunit-postgresql-bridge is not installed. Run "composer require --dev flow-php/phpunit-postgresql-bridge".',
                $name,
            ));
        }

        $container->getDefinition("flow.postgresql.{$name}.client")
            ->setFactory([StaticClient::class, 'connect']);
    }

    /**
     * @param array{service_id: string, clock_service_id: ?string, trace_queries: bool, trace_transactions: bool, collect_metrics: bool, log_queries: bool, max_query_length: int, include_parameters: bool, max_parameters: int, max_parameter_length: int} $telemetryConfig
     */
    private function registerTelemetry(string $name, array $telemetryConfig, ContainerBuilder $container) : void
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
