<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\DiffCommand;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationGenerator;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Diff\CatalogComparator;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class DiffCommandTest extends TestCase
{
    public function test_failure_when_no_changes(): void
    {
        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider(new Catalog([])),
            new FakeCatalogProvider(new Catalog([])),
            CatalogComparator::create(),
            new SpyMigrationGenerator(Version::fromString('20260401120000')),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.diff_generator', $diffGenerator);
        $container->set(
            'flow.postgresql.default.migrations.configuration',
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                '/tmp/migrations',
                'App\\Migrations',
            ),
        );

        $tester = new CommandTester(new DiffCommand($container, 'default'));
        $tester->execute(['name' => 'schema_change']);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString('No changes detected', $tester->getDisplay());
    }

    public function test_generates_diff(): void
    {
        $generator = new SpyMigrationGenerator(Version::fromString('20260401120000'));
        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider(new Catalog([])),
            new FakeCatalogProvider(new Catalog([])),
            CatalogComparator::create(),
            $generator,
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.diff_generator', $diffGenerator);
        $container->set(
            'flow.postgresql.default.migrations.configuration',
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                '/tmp/migrations',
                'App\\Migrations',
            ),
        );

        $tester = new CommandTester(new DiffCommand($container, 'default'));
        $tester->execute(['name' => 'schema_change', '--allow-empty-diff' => true]);

        static::assertStringContainsString('Generated migration: 20260401120000', $tester->getDisplay());
    }
}
