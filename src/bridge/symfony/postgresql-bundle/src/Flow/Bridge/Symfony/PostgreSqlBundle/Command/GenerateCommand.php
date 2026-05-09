<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use function Flow\Types\DSL\{type_instance_of, type_string};

use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;

use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:migrations:generate', description: 'Generate a blank migration class')]
final class GenerateCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure() : void
    {
        $this
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)
            ->addArgument('name', InputArgument::OPTIONAL, 'The name of the migration (e.g. \'add_email_index\')');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $generator = type_instance_of(MigrationGenerator::class)->assert($this->container->get("flow.postgresql.{$connection}.migrations.generator"));
        $configuration = type_instance_of(MigrationsConfiguration::class)->assert($this->container->get("flow.postgresql.{$connection}.migrations.configuration"));
        /** @var ?string $name */
        $name = $input->getArgument('name');

        $version = $generator->generateDataMigration($name);

        $directory = $configuration->migrationsDirectory . '/' . $version . ($name !== null ? '_' . $name : '');
        $realDirectory = \realpath($directory) ?: $directory;

        $io->success(\sprintf('Generated migration: %s', $version));

        $io->table(['File', 'Path'], [
            ['<fg=cyan>Migration</>', $realDirectory . '/' . $configuration->migrationFileName],
            ['<fg=cyan>Rollback</>', $realDirectory . '/' . $configuration->rollbackFileName],
        ]);

        return Command::SUCCESS;
    }
}
