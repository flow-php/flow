<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\{CreateDatabaseCommand, DropDatabaseCommand, GenerateCommand, RunSqlCommand, UpToDateCommand};
use Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\FlowPostgreSqlExtension;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\{AttributeTestCatalogProvider, TestKernel, VoidTelemetryFactory};
use Flow\PostgreSql\Client\Client;
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.migrations.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.migrations.catalog_provider'));

        $catalog = $this->getContainer()->get('flow.postgresql.migrations.catalog_provider')->get();
        self::assertTrue($catalog->get('public')->hasTable('attribute_test'));
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.migrations.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.migrations.catalog_provider'));

        $catalog = $this->getContainer()->get('flow.postgresql.migrations.catalog_provider')->get();
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

        self::assertTrue($this->getContainer()->has('flow.postgresql.migrations.catalog_provider'));
        self::assertInstanceOf(ChainCatalogProvider::class, $this->getContainer()->get('flow.postgresql.migrations.catalog_provider'));
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
