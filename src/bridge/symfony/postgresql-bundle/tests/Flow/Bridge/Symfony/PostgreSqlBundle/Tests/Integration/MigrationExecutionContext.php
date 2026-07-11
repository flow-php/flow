<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\SymfonyContext;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\MigrationSeedProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\SimpleTestCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\TestKernel;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Flow\PostgreSql\Migrations\Migrator;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function dirname;
use function getenv;
use function sprintf;

final class MigrationExecutionContext
{
    public const CONFIGURED_TABLE = 'flow_test_configured_context';

    public const CONTAINER_TABLE = 'flow_test_container_access';

    public string $dsn;

    public SymfonyContext $symfony;

    public function __construct()
    {
        $this->symfony = new SymfonyContext();
        $this->dsn = getenv('PGSQL_DATABASE_URL') ?: 'postgresql://postgres:postgres@localhost:5432/postgres';
        $this->dropTestTables();
    }

    public function bootConfiguredContext(): void
    {
        $this->symfony->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('flow_test.private_seed_provider', MigrationSeedProvider::class);
                    $container->register('test.catalog_provider', SimpleTestCatalogProvider::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => ['dsn' => $this->dsn],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => dirname(__DIR__) . '/Fixtures/migrations/configured_context',
                        'namespace' => 'FlowTest\\Migrations',
                        'table_name' => 'flow_migrations_test',
                        'context' => [
                            'table_name' => self::CONFIGURED_TABLE,
                            'seed_provider' => '@flow_test.private_seed_provider',
                        ],
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);
    }

    public function bootContainerAccess(): void
    {
        $this->symfony->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setParameter('flow_test.container_table', self::CONTAINER_TABLE);
                    $container
                        ->register('flow_test.public_seed_provider', MigrationSeedProvider::class)
                        ->setPublic(true);
                    $container->register('test.catalog_provider', SimpleTestCatalogProvider::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => ['dsn' => $this->dsn],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => dirname(__DIR__) . '/Fixtures/migrations/container_access',
                        'namespace' => 'FlowTest\\Migrations',
                        'table_name' => 'flow_migrations_test',
                    ],
                    'catalog_providers' => [
                        ['catalog_provider_id' => 'test.catalog_provider'],
                    ],
                ]);
            },
        ]);
    }

    public function fetchValue(string $table): string
    {
        $client = $this->connect();
        $value = $client->fetchScalarString(sprintf('SELECT value FROM %s LIMIT 1', $table));
        $client->close();

        return $value;
    }

    public function migrator(): Migrator
    {
        return $this->symfony->getService('flow.postgresql.migrations.migrator', Migrator::class);
    }

    public function shutdown(): void
    {
        $this->symfony->shutdown();
        $this->dropTestTables();
    }

    public function tableExists(string $table): bool
    {
        $client = $this->connect();
        $count = $client->fetchScalarInt('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2', [
            'public',
            $table,
        ]);
        $client->close();

        return $count > 0;
    }

    private function connect(): Client
    {
        return PgSqlClient::connect((new DsnParser())->parse($this->dsn));
    }

    private function dropTestTables(): void
    {
        $client = $this->connect();
        $client->execute(sprintf(
            'DROP TABLE IF EXISTS %s, %s, flow_migrations_test CASCADE',
            self::CONTAINER_TABLE,
            self::CONFIGURED_TABLE,
        ));
        $client->close();
    }
}
