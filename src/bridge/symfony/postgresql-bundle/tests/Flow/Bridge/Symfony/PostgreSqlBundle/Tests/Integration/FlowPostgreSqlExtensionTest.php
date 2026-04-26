<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\{CreateDatabaseCommand, DropDatabaseCommand, GenerateCommand, RunSqlCommand, UpToDateCommand};
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\FlowPostgreSqlExtension;
use Flow\Bridge\Symfony\PostgreSqlBundle\Messenger\FlowPostgreSqlTransportFactory;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double\CacheSpyClient;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\{AttributeTestCatalogProvider, TestKernel, VoidTelemetryFactory};
use Flow\Bridge\Symfony\PostgreSQLCache\{CacheCatalogProvider, FlowPostgreSqlCacheAdapter};
use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\PostgreSql\Client\{Client, Context};
use Flow\PostgreSql\Client\Telemetry\{PostgreSqlTelemetryOptions, TraceableClient};
use Flow\PostgreSql\Migrations\{Configuration as MigrationsConfiguration, MigrationsFactory, Migrator, VersionResolver};
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\{DiffMigrationGenerator, MigrationGenerator};
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, SpyClient};
use Flow\PostgreSql\Schema\{Catalog, ChainCatalogProvider};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

#[CoversClass(FlowPostgreSqlExtension::class)]
final class FlowPostgreSqlExtensionTest extends KernelTestCase
{
    public function test_attribute_based_catalog_provider_auto_discovery() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register(AttributeTestCatalogProvider::class, AttributeTestCatalogProvider::class)
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.catalog_provider'));

        $catalog = $this->getContainer()->get('flow.postgresql.catalog_provider')->get();
        self::assertTrue($catalog->get('public')->hasTable('attribute_test'));
    }

    public function test_cache_catalog_provider_is_merged_into_chain_when_migrations_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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
        self::assertTrue($chain->get()->get('public')->hasTable('cache_app'));
    }

    public function test_cache_pool_registers_adapter_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.cache.pool.app'));
        self::assertInstanceOf(FlowPostgreSqlCacheAdapter::class, $this->getContainer()->get('flow.postgresql.cache.pool.app'));
    }

    public function test_cache_pool_registers_catalog_provider_with_tag() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.cache.pool.sessions.catalog_provider'));

        /** @var CacheCatalogProvider $provider */
        $provider = $this->getContainer()->get('flow.postgresql.cache.pool.sessions.catalog_provider');
        self::assertInstanceOf(CacheCatalogProvider::class, $provider);

        $catalog = $provider->get();
        self::assertTrue($catalog->has('sess'));
        self::assertSame('session_cache', $catalog->get('sess')->tables[0]->name);
    }

    public function test_cache_pool_unknown_connection_throws() : void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('references unknown connection "missing"');

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

    public function test_cache_pool_uses_first_connection_when_connection_omitted() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.primary.client', CacheSpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.secondary.client', CacheSpyClient::class)->setPublic(true);
                });
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

        /** @var FlowPostgreSqlCacheAdapter $adapter */
        $adapter = $this->getContainer()->get('flow.postgresql.cache.pool.app');
        $adapter->getItem('probe');

        /** @var CacheSpyClient $primary */
        $primary = $this->getContainer()->get('flow.postgresql.primary.client');
        /** @var CacheSpyClient $secondary */
        $secondary = $this->getContainer()->get('flow.postgresql.secondary.client');

        self::assertNotSame([], $primary->executedQueries);
        self::assertSame([], $secondary->executedQueries);
    }

    public function test_cache_pool_uses_named_connection_when_specified() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.primary.client', CacheSpyClient::class)->setPublic(true);
                    $container->register('flow.postgresql.secondary.client', CacheSpyClient::class)->setPublic(true);
                });
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

        /** @var FlowPostgreSqlCacheAdapter $adapter */
        $adapter = $this->getContainer()->get('flow.postgresql.cache.pool.app');
        $adapter->getItem('probe');

        /** @var CacheSpyClient $primary */
        $primary = $this->getContainer()->get('flow.postgresql.primary.client');
        /** @var CacheSpyClient $secondary */
        $secondary = $this->getContainer()->get('flow.postgresql.secondary.client');

        self::assertSame([], $primary->executedQueries);
        self::assertNotSame([], $secondary->executedQueries);
    }

    public function test_cache_section_omitted_does_not_register_any_pool() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertFalse($this->getContainer()->has('flow.postgresql.cache.pool.app'));
    }

    public function test_catalog_providers_with_inline_catalog() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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
                                                    ['name' => 'id', 'type' => ['name' => 'int4', 'schema' => 'pg_catalog'], 'nullable' => false],
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.catalog_provider'));

        $catalog = $this->getContainer()->get('flow.postgresql.catalog_provider')->get();
        self::assertSame(['public'], $catalog->names());
        self::assertTrue($catalog->get('public')->hasTable('users'));
    }

    public function test_catalog_providers_with_service_reference() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.catalog_provider'));
    }

    public function test_class_aliases_for_migration_services() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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

        self::assertTrue($this->getContainer()->has(Migrator::class));
        self::assertTrue($this->getContainer()->has(MigrationStore::class));
        self::assertTrue($this->getContainer()->has(MigrationsFactory::class));
        self::assertTrue($this->getContainer()->has(MigrationsConfiguration::class));
        self::assertTrue($this->getContainer()->has(MigrationRepository::class));
        self::assertTrue($this->getContainer()->has(MigrationExecutor::class));
        self::assertTrue($this->getContainer()->has(VersionResolver::class));
        self::assertTrue($this->getContainer()->has(MigrationGenerator::class));
        self::assertTrue($this->getContainer()->has(DiffMigrationGenerator::class));
    }

    public function test_client_with_telemetry_creates_default_clock_when_not_specified() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container->register('test.telemetry', Telemetry::class)
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

        self::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertInstanceOf(SystemClock::class, $this->getContainer()->get('flow.postgresql.default.telemetry.clock'));
    }

    public function test_client_with_telemetry_is_decorated_with_traceable_client() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container->register('test.telemetry', Telemetry::class)
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

        self::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertInstanceOf(TraceableClient::class, $this->getContainer()->get(Client::class));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.telemetry.options'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.telemetry.config'));
    }

    public function test_client_with_telemetry_uses_custom_clock_service() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container->register('test.telemetry', Telemetry::class)
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

        self::assertInstanceOf(TraceableClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.clock'));
    }

    public function test_client_without_telemetry_is_not_decorated() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.options'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.default.telemetry.config'));
    }

    public function test_connection_with_migrations_registers_migration_services() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.configuration'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.factory'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.migrator'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.store'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.repository'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.generator'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.diff_generator'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.default.migrations.version_resolver'));
    }

    public function test_connection_without_migrations_has_no_migration_services() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertFalse($this->getContainer()->has('flow.postgresql.default.migrations.migrator'));
    }

    public function test_context_catalog_merged_with_user_data_when_both_configured() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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
                        ['catalog' => ['schemas' => [
                            ['name' => 'public', 'tables' => []],
                        ]]],
                    ],
                ]);
            },
        ]);

        $context = $this->getContainer()->get('flow.postgresql.default.context');

        self::assertInstanceOf(Context::class, $context);
        self::assertSame(42, $context->all()['tenant_id']);
        self::assertNotNull($context->catalog());
        self::assertSame(['public'], $context->catalog()->names());
    }

    public function test_context_exposes_shared_catalog_for_every_connection_when_catalog_providers_configured() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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
                        ['catalog' => ['schemas' => [
                            ['name' => 'public', 'tables' => []],
                        ]]],
                    ],
                ]);
            },
        ]);

        $defaultContext = $this->getContainer()->get('flow.postgresql.default.context');
        $analyticsContext = $this->getContainer()->get('flow.postgresql.analytics.context');

        self::assertInstanceOf(Context::class, $defaultContext);
        self::assertInstanceOf(Context::class, $analyticsContext);
        self::assertNotNull($defaultContext->catalog());
        self::assertNotNull($analyticsContext->catalog());
        self::assertSame(['public'], $defaultContext->catalog()->names());
        self::assertSame(['public'], $analyticsContext->catalog()->names());
    }

    public function test_context_is_not_registered_when_absent_from_config() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertFalse($this->getContainer()->has('flow.postgresql.default.context'));
    }

    public function test_context_service_is_registered_when_configured_on_connection() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertInstanceOf(Context::class, $context);
        self::assertSame(42, $context->all()['tenant_id']);
        self::assertSame(['a', 'b'], $context->all()['tags']);
        self::assertSame(3600, $context->all()['ttl']);
    }

    public function test_database_commands_registered_for_any_connection() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
            },
        ]);

        self::assertInstanceOf(CreateDatabaseCommand::class, $this->getContainer()->get('flow.postgresql.command.database_create'));
        self::assertInstanceOf(DropDatabaseCommand::class, $this->getContainer()->get('flow.postgresql.command.database_drop'));
        self::assertInstanceOf(RunSqlCommand::class, $this->getContainer()->get('flow.postgresql.command.sql_run'));
    }

    public function test_first_connection_gets_class_aliases() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get(Client::class));
    }

    public function test_messenger_catalog_provider_registered_when_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));

        $provider = $this->getContainer()->get('flow.postgresql.messenger.catalog_provider');
        self::assertInstanceOf(MessengerCatalogProvider::class, $provider);

        $catalog = $provider->get();
        self::assertTrue($catalog->has('messaging'));
        self::assertSame('custom_queue', $catalog->get('messaging')->tables[0]->name);
    }

    public function test_messenger_not_registered_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertFalse($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.messenger.transport_factory'));
    }

    public function test_messenger_transport_factory_has_access_to_all_connections() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.messenger.transport_factory'));
        self::assertInstanceOf(FlowPostgreSqlTransportFactory::class, $this->getContainer()->get('flow.postgresql.messenger.transport_factory'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.messenger.catalog_provider'));
    }

    public function test_migration_commands_registered_when_migrations_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.command.current'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.latest'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.status'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.list'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.migrate'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.execute'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.command.diff'));
    }

    public function test_migration_configuration_values_propagated() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);

        /** @var MigrationsConfiguration $configuration */
        $configuration = $this->getContainer()->get('flow.postgresql.default.migrations.configuration');

        self::assertSame('/tmp/custom_migrations', $configuration->migrationsDirectory);
        self::assertSame('Custom\\Migrations', $configuration->migrationsNamespace);
        self::assertSame('custom_migrations_table', $configuration->tableName);
        self::assertSame('custom_schema', $configuration->tableSchema);
    }

    public function test_migration_generate_and_up_to_date_commands_registered() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $catalogDef = new Definition(Catalog::class, [[]]);
                    $container->register('test.catalog_provider', FakeCatalogProvider::class)
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

        self::assertInstanceOf(GenerateCommand::class, $this->getContainer()->get('flow.postgresql.command.generate'));
        self::assertInstanceOf(UpToDateCommand::class, $this->getContainer()->get('flow.postgresql.command.up_to_date'));
    }

    public function test_multiple_connections_register_separate_clients() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.default.client'));
        self::assertTrue($this->getContainer()->has('flow.postgresql.reporting.client'));
        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.reporting.client'));
    }

    public function test_no_generate_command_without_migrations() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => 'postgresql://postgres:postgres@localhost:5432/postgres',
                        ],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                });
            },
        ]);

        self::assertFalse($this->getContainer()->has('flow.postgresql.command.generate'));
    }

    public function test_no_migration_commands_when_no_migrations() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertFalse($this->getContainer()->has('flow.postgresql.command.current'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.latest'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.status'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.list'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.migrate'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.execute'));
        self::assertFalse($this->getContainer()->has('flow.postgresql.command.diff'));
    }

    public function test_single_connection_registers_client() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
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

        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get('flow.postgresql.default.client'));
        self::assertInstanceOf(SpyClient::class, $this->getContainer()->get(Client::class));
    }

    public function test_telemetry_options_propagated_to_config() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) : void {
                    $container->register('flow.postgresql.default.client', SpyClient::class)->setPublic(true);
                    $container->register('test.telemetry.factory', VoidTelemetryFactory::class);
                    $container->register('test.telemetry', Telemetry::class)
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
        self::assertInstanceOf(PostgreSqlTelemetryOptions::class, $options);
        self::assertTrue($options->logQueries);
        self::assertTrue($options->includeParameters);
        self::assertSame(500, $options->maxQueryLength);
        self::assertTrue($options->traceQueries);
        self::assertTrue($options->traceTransactions);
        self::assertTrue($options->collectMetrics);
    }
}
