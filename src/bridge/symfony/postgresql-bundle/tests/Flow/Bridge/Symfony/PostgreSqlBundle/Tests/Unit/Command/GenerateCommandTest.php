<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\GenerateCommand;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationGenerator;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class GenerateCommandTest extends TestCase
{
    public function test_generates_data_migration(): void
    {
        $version = Version::fromString('20260401120000');
        $generator = new SpyMigrationGenerator($version);

        $configuration = new Configuration(
            new SpyClient(),
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );

        $tester = new CommandTester(new GenerateCommand($generator, $configuration));
        $tester->execute(['name' => 'add_email_index']);

        static::assertSame(0, $tester->getStatusCode());
        static::assertStringContainsString('20260401120000', $tester->getDisplay());
        static::assertSame('add_email_index', $generator->lastDataName);
    }
}
