<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context\SymfonyContext;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\SimpleTestCatalogProvider;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\TestKernel;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\PostgreSql\Client\DsnParser;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use LogicException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;

use function bin2hex;
use function dirname;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function getenv;
use function is_string;
use function mkdir;
use function preg_match;
use function preg_replace;
use function random_bytes;
use function sprintf;

final class CommandTestContext
{
    public NativeLocalFilesystem $filesystem;

    public string $migrationsDir;

    public SymfonyContext $symfonyContext;

    public string $testDsn;

    public function __construct()
    {
        $this->symfonyContext = new SymfonyContext();
        $this->filesystem = native_local_filesystem();
        $this->testDsn = getenv('PGSQL_DATABASE_URL') ?: 'postgresql://postgres:postgres@localhost:5432/postgres';
        $this->migrationsDir = dirname(__DIR__, 8) . '/var/tests/flow_test_migrations_' . bin2hex(random_bytes(4));
        mkdir($this->migrationsDir, 0755, true);

        $client = PgSqlClient::connect((new DsnParser())->parse($this->testDsn));
        $client->execute('DROP TABLE IF EXISTS test_users, flow_migrations_test CASCADE');
        $client->close();
    }

    public function bootForDatabaseManagement(string $targetDatabase): void
    {
        $targetDsn = preg_replace('#/[^/?]+(\?|$)#', '/' . $targetDatabase . '$1', $this->testDsn);

        $this->symfonyContext->bootKernel([
            'config' => static function (TestKernel $kernel) use ($targetDsn): void {
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => $targetDsn,
                        ],
                    ],
                ]);
            },
        ]);
    }

    public function bootWithMigrations(): void
    {
        $this->symfonyContext->bootKernel([
            'config' => function (TestKernel $kernel): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('test.catalog_provider', SimpleTestCatalogProvider::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => $this->testDsn,
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => $this->migrationsDir,
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

    public function bootWithMigrationsForDatabase(string $database): void
    {
        $targetDsn = preg_replace('#/[^/?]+(\?|$)#', '/' . $database . '$1', $this->testDsn);

        $this->symfonyContext->bootKernel([
            'config' => function (TestKernel $kernel) use ($targetDsn): void {
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->register('test.catalog_provider', SimpleTestCatalogProvider::class)->setPublic(true);
                });
                $kernel->addTestExtensionConfig('flow_postgresql', [
                    'connections' => [
                        'default' => [
                            'dsn' => $targetDsn,
                        ],
                    ],
                    'migrations' => [
                        'enabled' => true,
                        'directory' => $this->migrationsDir,
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

    public function command(string $serviceId): Command
    {
        $command = $this->container()->get($serviceId);

        if (!$command instanceof Command) {
            throw new LogicException(sprintf('Service "%s" is not a Command instance.', $serviceId));
        }

        return $command;
    }

    public function container(): ContainerInterface
    {
        return $this->symfonyContext->getContainer();
    }

    public function createDatabase(string $name): void
    {
        $client = PgSqlClient::connect(
            (new DsnParser())
                ->parse($this->testDsn)
                ->withDatabase('postgres'),
        );
        $client->execute("CREATE DATABASE \"{$name}\"");
        $client->close();
    }

    public function databaseExists(string $name): bool
    {
        $client = PgSqlClient::connect(
            (new DsnParser())
                ->parse($this->testDsn)
                ->withDatabase('postgres'),
        );
        $count = $client->fetchScalarInt('SELECT COUNT(*) FROM pg_database WHERE datname = $1', [$name]);
        $client->close();

        return $count > 0;
    }

    public function dropDatabase(string $name): void
    {
        $client = PgSqlClient::connect(
            (new DsnParser())
                ->parse($this->testDsn)
                ->withDatabase('postgres'),
        );
        $client->execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()', [
            $name,
        ]);
        $client->execute("DROP DATABASE IF EXISTS \"{$name}\"");
        $client->close();
    }

    public function fileContent(string $filePath): string
    {
        return $this->filesystem->readFrom(path($filePath))->content();
    }

    public function fileExists(string $filePath): bool
    {
        return $this->filesystem->status(path($filePath)) !== null;
    }

    public function generateDiffMigration(): string
    {
        $command = $this->command('flow.postgresql.command.diff');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'create_test_users']);

        $matches = [];
        preg_match('/Generated migration: (\S+)/', $tester->getDisplay(), $matches);

        return $matches[1];
    }

    public function migrationDirs(string $pattern): array
    {
        $dirs = [];

        foreach ($this->filesystem->list(path($this->migrationsDir . '/' . $pattern), new KeepAll()) as $fileStatus) {
            if ($fileStatus->isDirectory()) {
                $dirs[] = $fileStatus->path->path();
            }
        }

        return $dirs;
    }

    public function runMigrate(): void
    {
        $command = $this->command('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute([]);
    }

    public function shutdown(): void
    {
        $this->symfonyContext->shutdown();

        $client = PgSqlClient::connect((new DsnParser())->parse($this->testDsn));
        $client->execute('DROP TABLE IF EXISTS test_users, flow_migrations_test, flow_sql_test CASCADE');
        $client->close();

        $this->filesystem->rm(path($this->migrationsDir));
    }

    public function tableExists(string $table, string $schema = 'public'): bool
    {
        $client = PgSqlClient::connect((new DsnParser())->parse($this->testDsn));
        $result = $client->fetch('SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2', [
            $schema,
            $table,
        ]);
        $client->close();

        return $result !== null;
    }

    public function tableExistsInDatabase(string $database, string $table, string $schema = 'public'): bool
    {
        $targetDsn = preg_replace('#/[^/?]+(\?|$)#', '/' . $database . '$1', $this->testDsn);

        if (!is_string($targetDsn)) {
            throw new LogicException('Failed to build target DSN.');
        }
        $client = PgSqlClient::connect((new DsnParser())->parse($targetDsn));
        $result = $client->fetch('SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2', [
            $schema,
            $table,
        ]);
        $client->close();

        return $result !== null;
    }
}
