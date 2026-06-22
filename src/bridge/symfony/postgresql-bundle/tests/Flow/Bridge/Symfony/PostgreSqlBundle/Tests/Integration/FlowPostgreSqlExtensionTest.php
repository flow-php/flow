<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\CreateDatabaseCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\DropDatabaseCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\GenerateCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\RunSqlCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\SessionPurgeCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\UpToDateCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Messenger\FlowPostgreSqlTransportFactory;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\AttributeTestCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\MigrationSeedProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\VoidTelemetryFactory;
use Flow\Bridge\Symfony\PostgreSQLCache\CacheCatalogProvider;
use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;
use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\Bridge\Symfony\PostgreSQLSession\SessionCatalogProvider;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Context;
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
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\VersionResolver;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\ChainCatalogProvider;
use Flow\PostgreSql\Schema\Exclusion\AnyExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObject;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

#[CoversClass(FlowPostgreSqlBundle::class)]
final class FlowPostgreSqlExtensionTest extends KernelTestCase
{
    public function test_attribute_based_catalog_provider_auto_discovery(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container
                        ->register(AttributeTestCatalogProvider::class, AttributeTestCatalogProvider::class)
                        ->setAutoconfigured(true)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        static::assertInstanceOf(
            ChainCatalogProvider::class,
            $this->getContainer()->get('flow.postgresql.catalog_provider'),
        );

        $catalog = $this->getContainer()->get('flow.postgresql.catalog_provider')->get();
        static::assertTrue($catalog->get('public')->hasTable('attribute_test'));
    }

    public function test_cache_catalog_provider_is_merged_into_chain_when_migrations_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [
                                'table_name' => 'cache_app',
                            ],
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                ]);
            },
        ]);

        /** @var ChainCatalogProvider $chain */
        $chain = $this->getContainer()->get('flow.postgresql.catalog_provider');
        static::assertTrue($chain->get()->get('public')->hasTable('cache_app'));
    }

    public function test_cache_pool_registers_adapter_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.cache.pool.app'));
        static::assertInstanceOf(
            FlowPostgreSqlCacheAdapter::class,
            $this->getContainer()->get('flow.postgresql.cache.pool.app'),
        );
    }

    public function test_cache_pool_registers_catalog_provider_with_tag(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'sessions' => [
                                'table_name' => 'session_cache',
                                'schema' => 'sess',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.cache.pool.sessions.catalog_provider'));

        /** @var CacheCatalogProvider $provider */
        $provider = $this->getContainer()->get('flow.postgresql.cache.pool.sessions.catalog_provider');
        static::assertInstanceOf(CacheCatalogProvider::class, $provider);

        $catalog = $provider->get();
        static::assertTrue($catalog->has('sess'));
        static::assertSame('session_cache', $catalog->get('sess')->tables[0]->name);
    }

    public function test_cache_pool_share_connection_wires_client_reference(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => ['share_connection' => true],
                        ],
                    ],
                ]);
            },
        ]);

        $sharedClient = $this->getContainer()->get('flow.postgresql.default.client');
        $adapter = $this->getContainer()->get('flow.postgresql.cache.pool.app');

        static::assertSame($sharedClient, $this->symfonyContext()->readPrivateProperty($adapter, 'client'));
        static::assertNull($this->symfonyContext()->readPrivateProperty($adapter, 'connectionParameters'));
    }

    public function test_cache_pool_unknown_connection_throws(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('references unknown connection "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'broken' => [
                                'connection' => 'missing',
                            ],
                        ],
                    ],
                ]);
            },
        ]);
    }

    public function test_cache_pool_uses_first_connection_when_connection_omitted(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'primary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/primary',
                        ],
                        'secondary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/secondary',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => [],
                        ],
                    ],
                ]);
            },
        ]);

        $expected = $this->getContainer()->get('flow.postgresql.primary.connection_parameters');
        $adapter = $this->getContainer()->get('flow.postgresql.cache.pool.app');

        static::assertSame($expected, $this->symfonyContext()->readPrivateProperty($adapter, 'connectionParameters'));
    }

    public function test_cache_pool_uses_named_connection_when_specified(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'primary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/primary',
                        ],
                        'secondary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/secondary',
                        ],
                    ],
                    'cache' => [
                        'pools' => [
                            'app' => ['connection' => 'secondary'],
                        ],
                    ],
                ]);
            },
        ]);

        $expected = $this->getContainer()->get('flow.postgresql.secondary.connection_parameters');
        $adapter = $this->getContainer()->get('flow.postgresql.cache.pool.app');

        static::assertSame($expected, $this->symfonyContext()->readPrivateProperty($adapter, 'connectionParameters'));
    }

    public function test_cache_section_omitted_does_not_register_any_pool(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.cache.pool.app'));
    }

    public function test_catalog_providers_with_inline_catalog(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        [
                            'catalog' => [
                                'schemas' => [
                                    [
                                        'name' => 'public',
                                        'tables' => [
                                            [
                                                'name' => 'users',
                                                'columns' => [
                                                    [
                                                        'name' => 'id',
                                                        'type' => ['name' => 'int4', 'schema' => 'pg_catalog'],
                                                        'nullable' => false,
                                                    ],
                                                ],
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        static::assertInstanceOf(
            ChainCatalogProvider::class,
            $this->getContainer()->get('flow.postgresql.catalog_provider'),
        );

        $catalog = $this->getContainer()->get('flow.postgresql.catalog_provider')->get();
        static::assertSame(['public'], $catalog->names());
        static::assertTrue($catalog->get('public')->hasTable('users'));
    }

    public function test_catalog_providers_with_service_reference(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        static::assertInstanceOf(
            ChainCatalogProvider::class,
            $this->getContainer()->get('flow.postgresql.catalog_provider'),
        );
    }

    public function test_class_aliases_for_migration_services(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has(Migrator::class));
        static::assertTrue($this->getContainer()->has(MigrationStore::class));
        static::assertTrue($this->getContainer()->has(MigrationsFactory::class));
        static::assertTrue($this->getContainer()->has(MigrationsConfiguration::class));
        static::assertTrue($this->getContainer()->has(MigrationRepository::class));
        static::assertTrue($this->getContainer()->has(MigrationExecutor::class));
        static::assertTrue($this->getContainer()->has(VersionResolver::class));
        static::assertTrue($this->getContainer()->has(MigrationGenerator::class));
        static::assertTrue($this->getContainer()->has(DiffMigrationGenerator::class));
    }

    public function test_migrations_configuration_injects_service_container_attribute(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog' => ['schemas' => []]],
                    ],
                ]);
            },
        ]);

        $configuration = $this->getContainer()->get('flow.postgresql.migrations.configuration');
        static::assertInstanceOf(MigrationsConfiguration::class, $configuration);

        static::assertArrayHasKey(FlowPostgreSqlBundle::SERVICE_CONTAINER, $configuration->attributes);
        static::assertInstanceOf(
            ContainerInterface::class,
            $configuration->attributes[FlowPostgreSqlBundle::SERVICE_CONTAINER],
        );
    }

    public function test_migrations_configuration_merges_configured_context_attributes(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('app.report_generator', MigrationSeedProvider::class);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                        'context' => [
                            'report_generator' => '@app.report_generator',
                            'batch_size' => 500,
                        ],
                    ],
                    'catalog_providers' => [
                        ['catalog' => ['schemas' => []]],
                    ],
                ]);
            },
        ]);

        $configuration = $this->getContainer()->get('flow.postgresql.migrations.configuration');
        static::assertInstanceOf(MigrationsConfiguration::class, $configuration);

        static::assertInstanceOf(MigrationSeedProvider::class, $configuration->attributes['report_generator']);
        static::assertSame(500, $configuration->attributes['batch_size']);
        static::assertInstanceOf(
            ContainerInterface::class,
            $configuration->attributes[FlowPostgreSqlBundle::SERVICE_CONTAINER],
        );
    }

    public function test_migrations_exclude_config_builds_exclusion_policy(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                        'exclude' => [
                            ['schema' => 'tenant_data'],
                            ['starts_with' => 'user_upload_'],
                            ['ends_with' => '_tmp', 'type' => 'view'],
                            ['pattern' => '/^cache_\d+$/', 'type' => 'sequence'],
                            ['table' => 'legacy_audit'],
                        ],
                    ],
                    'catalog_providers' => [
                        ['catalog' => ['schemas' => []]],
                    ],
                ]);
            },
        ]);

        $configuration = $this->getContainer()->get('flow.postgresql.migrations.configuration');
        static::assertInstanceOf(MigrationsConfiguration::class, $configuration);

        $policy = $configuration->exclusionPolicy;
        static::assertInstanceOf(AnyExclusionPolicy::class, $policy);

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'tenant_data', 'uploads')));
        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'user_upload_9')));

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::VIEW, 'public', 'report_tmp')));
        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'report_tmp')));

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'cache_42')));
        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'cache_42')));

        static::assertTrue($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'legacy_audit')));
        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::SEQUENCE, 'public', 'legacy_audit')));

        static::assertFalse($policy->exclude(new SchemaObject(SchemaObjectType::TABLE, 'public', 'orders')));
    }

    public function test_migrations_exclude_entry_without_matcher_throws(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('must define exactly one of');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                        'exclude' => [
                            ['for_schema' => 'public'],
                        ],
                    ],
                ]);
            },
        ]);
    }

    public function test_client_with_telemetry_creates_default_clock_when_not_specified(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container
                        ->register('test.telemetry', Telemetry::class)
                        ->setFactory([new Reference('test.telemetry.factory'), 'create'])
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'telemetry' => [
                                'service_id' => 'test.telemetry',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertInstanceOf(
            SystemClock::class,
            $this->getContainer()->get('flow.postgresql.default.telemetry.clock'),
        );
    }

    public function test_client_with_telemetry_is_decorated_with_traceable_client(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container
                        ->register('test.telemetry', Telemetry::class)
                        ->setFactory([new Reference('test.telemetry.factory'), 'create'])
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'telemetry' => [
                                'service_id' => 'test.telemetry',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertInstanceOf(TraceableClient::class, $this->getContainer()->get(Client::class));
        static::assertTrue($this->getContainer()->has('flow.postgresql.default.telemetry.options'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.default.telemetry.config'));
    }

    public function test_client_with_telemetry_uses_custom_clock_service(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container
                        ->register('test.telemetry', Telemetry::class)
                        ->setFactory([new Reference('test.telemetry.factory'), 'create'])
                        ->setPublic(true);
                    $container->register('test.clock', SystemClock::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'telemetry' => [
                                'service_id' => 'test.telemetry',
                                'clock_service_id' => 'test.clock',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.clock'));
    }

    public function test_client_without_telemetry_is_not_decorated(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.options'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.config'));
    }

    public function test_connection_only_dsn_behaves_as_before(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/app',
                        ],
                    ],
                ]);
            },
        ]);

        $params = $this->getContainer()->get('flow.postgresql.default.connection_parameters');
        static::assertSame('app', $params->database());
        static::assertSame('localhost', $params->host());
    }

    public function test_connection_parameters_apply_dbname_override_before_suffix(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/app',
                            'dbname' => 'other',
                            'dbname_suffix' => '_test',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertSame(
            'other_test',
            $this->getContainer()->get('flow.postgresql.default.connection_parameters')->database(),
        );
    }

    public function test_connection_parameters_apply_dsn_part_overrides(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://user:pass@localhost:5432/app',
                            'host' => 'db.internal',
                            'port' => 5544,
                            'user' => 'svc',
                            'password' => 'pw',
                        ],
                    ],
                ]);
            },
        ]);

        $params = $this->getContainer()->get('flow.postgresql.default.connection_parameters');
        static::assertSame('db.internal', $params->host());
        static::assertSame(5544, $params->port());
        static::assertSame('svc', $params->user());
        static::assertSame('pw', $params->password());
    }

    public function test_connection_parameters_apply_dbname_suffix(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/app',
                            'dbname_suffix' => '_test',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertSame(
            'app_test',
            $this->getContainer()->get('flow.postgresql.default.connection_parameters')->database(),
        );
    }

    public function test_connection_parameters_resolve_dbname_suffix_from_env(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('env(FLOW_TEST_DB_SUFFIX)', '_test7');
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/app',
                            'dbname_suffix' => '%env(FLOW_TEST_DB_SUFFIX)%',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertSame(
            'app_test7',
            $this->getContainer()->get('flow.postgresql.default.connection_parameters')->database(),
        );
    }

    public function test_connection_with_migrations_registers_migration_services(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.configuration'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.factory'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.migrator'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.store'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.repository'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.generator'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.diff_generator'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.version_resolver'));
    }

    public function test_connection_without_migrations_has_no_migration_services(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.migrations.migrator'));
    }

    public function test_migrations_use_configured_connection(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.secondary.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => ['dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres'],
                        'secondary' => ['dsn' => 'postgresql://postgres:postgres@localhost:5432/secondary'],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'connection' => 'secondary',
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.migrations.migrator'));
        static::assertSame('secondary', $this->getContainer()->getParameter('flow.postgresql.migrations.connection'));
    }

    public function test_migrations_throw_when_configured_connection_is_unknown(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Migrations are configured to run against connection "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => ['dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres'],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'connection' => 'missing',
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                ]);
            },
        ]);
    }

    public function test_context_catalog_merged_with_user_data_when_both_configured(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'context' => [
                                'tenant_id' => 42,
                            ],
                        ],
                    ],
                    'catalog_providers' => [
                        [
                            'catalog' => [
                                'schemas' => [
                                    ['name' => 'public', 'tables' => []],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $context = $this->getContainer()->get('flow.postgresql.default.context');

        static::assertInstanceOf(Context::class, $context);
        static::assertSame(42, $context->all()['tenant_id']);
        static::assertNotNull($context->catalog());
        static::assertSame(['public'], $context->catalog()->names());
    }

    public function test_context_exposes_shared_catalog_for_every_connection_when_catalog_providers_configured(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.analytics.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                        'analytics' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/analytics',
                        ],
                    ],
                    'catalog_providers' => [
                        [
                            'catalog' => [
                                'schemas' => [
                                    ['name' => 'public', 'tables' => []],
                                ],
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $defaultContext = $this->getContainer()->get('flow.postgresql.default.context');
        $analyticsContext = $this->getContainer()->get('flow.postgresql.analytics.context');

        static::assertInstanceOf(Context::class, $defaultContext);
        static::assertInstanceOf(Context::class, $analyticsContext);
        static::assertNotNull($defaultContext->catalog());
        static::assertNotNull($analyticsContext->catalog());
        static::assertSame(['public'], $defaultContext->catalog()->names());
        static::assertSame(['public'], $analyticsContext->catalog()->names());
    }

    public function test_context_is_not_registered_when_absent_from_config(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.default.context'));
    }

    public function test_context_service_is_registered_when_configured_on_connection(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->setParameter('app.cache_ttl', 3600);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'context' => [
                                'tenant_id' => 42,
                                'tags' => ['a', 'b'],
                                'ttl' => '%app.cache_ttl%',
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        $context = $this->getContainer()->get('flow.postgresql.default.context');

        static::assertInstanceOf(Context::class, $context);
        static::assertSame(42, $context->all()['tenant_id']);
        static::assertSame(['a', 'b'], $context->all()['tags']);
        static::assertSame(3600, $context->all()['ttl']);
    }

    public function test_database_commands_registered_for_any_connection(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
            },
        ]);

        static::assertInstanceOf(
            CreateDatabaseCommand::class,
            $this->getContainer()->get('flow.postgresql.command.database_create'),
        );
        static::assertInstanceOf(
            DropDatabaseCommand::class,
            $this->getContainer()->get('flow.postgresql.command.database_drop'),
        );
        static::assertInstanceOf(RunSqlCommand::class, $this->getContainer()->get('flow.postgresql.command.sql_run'));
    }

    public function test_first_connection_gets_class_aliases(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.reporting.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                        'reporting' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/reporting',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get(Client::class));
    }

    public function test_messenger_catalog_provider_registered_when_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'messenger' => [
                        'enabled' => true,
                        'table_name' => 'custom_queue',
                        'schema' => 'messaging',
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));

        $provider = $this->getContainer()->get('flow.postgresql.messenger.catalog_provider');
        static::assertInstanceOf(MessengerCatalogProvider::class, $provider);

        $catalog = $provider->get();
        static::assertTrue($catalog->has('messaging'));
        static::assertSame('custom_queue', $catalog->get('messaging')->tables[0]->name);
    }

    public function test_messenger_not_registered_when_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.messenger.transport_factory'));
    }

    public function test_messenger_transport_factory_has_access_to_all_connections(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.analytics.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                        'analytics' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/analytics',
                        ],
                    ],
                    'messenger' => [
                        'enabled' => true,
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.messenger.transport_factory'));
        static::assertInstanceOf(
            FlowPostgreSqlTransportFactory::class,
            $this->getContainer()->get('flow.postgresql.messenger.transport_factory'),
        );
        static::assertTrue($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));
    }

    public function test_migration_commands_registered_when_migrations_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.command.current'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.latest'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.status'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.list'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.migrate'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.execute'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.command.diff'));
    }

    public function test_migration_configuration_values_propagated(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/custom_migrations',
                        'namespace' => 'Custom\\Migrations',
                        'table_name' => 'custom_migrations_table',
                        'table_schema' => 'custom_schema',
                        'drop_if_exists' => true,
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        /** @var MigrationsConfiguration $configuration */
        $configuration = $this->getContainer()->get('flow.postgresql.migrations.configuration');

        static::assertSame('/tmp/custom_migrations', $configuration->migrationsDirectory);
        static::assertSame('Custom\\Migrations', $configuration->migrationsNamespace);
        static::assertSame('custom_migrations_table', $configuration->tableName);
        static::assertSame('custom_schema', $configuration->tableSchema);
        static::assertTrue($configuration->dropIfExists);
    }

    public function test_migration_generate_and_up_to_date_commands_registered(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container
                        ->register('test.catalog_provider', FakeCatalogProvider::class)
                        ->addArgument($catalogDef)
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(
            GenerateCommand::class,
            $this->getContainer()->get('flow.postgresql.command.generate'),
        );
        static::assertInstanceOf(
            UpToDateCommand::class,
            $this->getContainer()->get('flow.postgresql.command.up_to_date'),
        );
    }

    public function test_multiple_connections_register_separate_clients(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.reporting.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                        'reporting' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/reporting',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.default.client'));
        static::assertTrue($this->getContainer()->has('flow.postgresql.reporting.client'));
        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.reporting.client'));
    }

    public function test_no_generate_command_without_migrations(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.command.generate'));
    }

    public function test_no_migration_commands_when_no_migrations(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.command.current'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.latest'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.status'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.list'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.migrate'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.execute'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.command.diff'));
    }

    public function test_session_catalog_provider_is_merged_into_chain_when_migrations_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                        'table_name' => 'app_sessions',
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => '/tmp/test_migrations',
                        'namespace' => 'App\\Migrations',
                    ],
                ]);
            },
        ]);

        /** @var ChainCatalogProvider $chain */
        $chain = $this->getContainer()->get('flow.postgresql.catalog_provider');
        static::assertTrue($chain->get()->get('public')->hasTable('app_sessions'));
    }

    public function test_session_handler_registers_when_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.session.handler'));
        static::assertInstanceOf(
            FlowPostgreSqlSessionHandler::class,
            $this->getContainer()->get('flow.postgresql.session.handler'),
        );
    }

    public function test_session_not_registered_when_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertFalse($this->getContainer()->has('flow.postgresql.session.handler'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.session.catalog_provider'));
        static::assertFalse($this->getContainer()->has('flow.postgresql.session.purge_command'));
    }

    public function test_session_purge_command_registered_when_session_enabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.session.purge_command'));
        static::assertInstanceOf(
            SessionPurgeCommand::class,
            $this->getContainer()->get('flow.postgresql.session.purge_command'),
        );
    }

    public function test_session_registers_catalog_provider_with_tag(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                        'table_name' => 'tagged_sessions',
                        'schema' => 'sess',
                    ],
                ]);
            },
        ]);

        static::assertTrue($this->getContainer()->has('flow.postgresql.session.catalog_provider'));
        $provider = $this->getContainer()->get('flow.postgresql.session.catalog_provider');
        static::assertInstanceOf(SessionCatalogProvider::class, $provider);

        $catalog = $provider->get();
        static::assertTrue($catalog->has('sess'));
        static::assertSame('tagged_sessions', $catalog->get('sess')->tables[0]->name);
    }

    public function test_session_share_connection_wires_client_reference(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                        'share_connection' => true,
                    ],
                ]);
            },
        ]);

        $sharedClient = $this->getContainer()->get('flow.postgresql.default.client');
        $handler = $this->getContainer()->get('flow.postgresql.session.handler');

        static::assertSame($sharedClient, $this->symfonyContext()->readPrivateProperty($handler, 'client'));
        static::assertNull($this->symfonyContext()->readPrivateProperty($handler, 'connectionParameters'));
    }

    public function test_session_unknown_connection_throws(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Session references unknown connection "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                        'connection' => 'missing',
                    ],
                ]);
            },
        ]);
    }

    public function test_session_uses_first_connection_when_connection_omitted(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'primary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/primary',
                        ],
                        'secondary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/secondary',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                    ],
                ]);
            },
        ]);

        $expected = $this->getContainer()->get('flow.postgresql.primary.connection_parameters');
        $handler = $this->getContainer()->get('flow.postgresql.session.handler');

        static::assertSame($expected, $this->symfonyContext()->readPrivateProperty($handler, 'connectionParameters'));
    }

    public function test_session_uses_named_connection_when_specified(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'primary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/primary',
                        ],
                        'secondary' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/secondary',
                        ],
                    ],
                    'session' => [
                        'enabled' => true,
                        'connection' => 'secondary',
                    ],
                ]);
            },
        ]);

        $expected = $this->getContainer()->get('flow.postgresql.secondary.connection_parameters');
        $handler = $this->getContainer()->get('flow.postgresql.session.handler');

        static::assertSame($expected, $this->symfonyContext()->readPrivateProperty($handler, 'connectionParameters'));
    }

    public function test_single_connection_registers_client(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
            },
        ]);

        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        static::assertInstanceOf(SpyClient::class, $this->getContainer()->get(Client::class));
    }

    public function test_telemetry_options_propagated_to_config(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container
                        ->register('test.telemetry', Telemetry::class)
                        ->setFactory([new Reference('test.telemetry.factory'), 'create'])
                        ->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                            'telemetry' => [
                                'service_id' => 'test.telemetry',
                                'log_queries' => true,
                                'include_parameters' => true,
                                'max_query_length' => 500,
                            ],
                        ],
                    ],
                ]);
            },
        ]);

        /** @var PostgreSqlTelemetryOptions $options */
        $options = $this->getContainer()->get('flow.postgresql.default.telemetry.options');
        static::assertInstanceOf(PostgreSqlTelemetryOptions::class, $options);
        static::assertTrue($options->logQueries);
        static::assertTrue($options->includeParameters);
        static::assertSame(500, $options->maxQueryLength);
        static::assertTrue($options->traceQueries);
        static::assertTrue($options->traceTransactions);
        static::assertTrue($options->collectMetrics);
    }
}
