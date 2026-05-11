<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

#[AsCommand(
    name: 'flow:migrations:diff',
    description: 'Generate a migration by comparing the current database to the catalog',
)]
final class DiffCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)
            ->addArgument('name', InputArgument::OPTIONAL, 'The name of the migration (e.g. \'add_categories\')')
            ->addOption('allow-empty-diff', null, InputOption::VALUE_NONE, 'Do not throw when no changes are detected')
            ->addOption('from-empty-schema', null, InputOption::VALUE_NONE, 'Generate as if the database were empty');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $diffGenerator = type_instance_of(DiffMigrationGenerator::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.diff_generator",
        ));
        $configuration = type_instance_of(MigrationsConfiguration::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.configuration",
        ));
        /** @var ?string $name */
        $name = $input->getArgument('name');
        $allowEmpty = (bool) $input->getOption('allow-empty-diff');
        $fromEmpty = (bool) $input->getOption('from-empty-schema');

        try {
            $version = $diffGenerator->generate($name, $allowEmpty, $fromEmpty);
        } catch (MigrationException $e) {
            $io->warning($e->getMessage());

            return Command::FAILURE;
        }

        $directory = $configuration->migrationsDirectory . '/' . $version . ($name !== null ? '_' . $name : '');
        $realDirectory = \realpath($directory) ?: $directory;
        $migrationPath = $realDirectory . '/' . $configuration->migrationFileName;
        $rollbackPath = $realDirectory . '/' . $configuration->rollbackFileName;

        $io->success(\sprintf('Generated migration: %s', $version));

        $rows = [['<fg=cyan>Migration</>', $migrationPath]];

        if (\file_exists($rollbackPath)) {
            $rows[] = ['<fg=cyan>Rollback</>', $rollbackPath];
        }

        $io->table(['File', 'Path'], $rows);

        return Command::SUCCESS;
    }
}
