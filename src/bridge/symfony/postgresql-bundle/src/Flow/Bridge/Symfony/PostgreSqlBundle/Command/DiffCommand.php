<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function file_exists;
use function realpath;
use function sprintf;

#[AsCommand(
    name: 'flow:migrations:diff',
    description: 'Generate a migration by comparing the current database to the catalog',
)]
final class DiffCommand extends Command
{
    public function __construct(
        private readonly DiffMigrationGenerator $diffGenerator,
        private readonly MigrationsConfiguration $configuration,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('name', InputArgument::OPTIONAL, 'The name of the migration (e.g. \'add_categories\')')
            ->addOption('allow-empty-diff', null, InputOption::VALUE_NONE, 'Do not throw when no changes are detected')
            ->addOption('from-empty-schema', null, InputOption::VALUE_NONE, 'Generate as if the database were empty')
            ->addOption(
                'drop-if-exists',
                null,
                InputOption::VALUE_NONE,
                'Emit IF EXISTS on generated DROP statements (overrides the migrations.drop_if_exists config for this run)',
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        /** @var ?string $name */
        $name = $input->getArgument('name');
        $allowEmpty = $input->getOption('allow-empty-diff') === true;
        $fromEmpty = $input->getOption('from-empty-schema') === true;
        $dropIfExists = $input->getOption('drop-if-exists') === true ? true : null;

        try {
            $version = $this->diffGenerator->generate($name, $allowEmpty, $fromEmpty, $dropIfExists);
        } catch (MigrationException $e) {
            $io->warning($e->getMessage());

            return Command::FAILURE;
        }

        $directory =
            $this->configuration->migrationsDirectory . '/' . (string) $version . ($name !== null ? '_' . $name : '');
        $realDirectory = realpath($directory) ?: $directory;
        $migrationPath = $realDirectory . '/' . $this->configuration->migrationFileName;
        $rollbackPath = $realDirectory . '/' . $this->configuration->rollbackFileName;

        $io->success(sprintf('Generated migration: %s', $version));

        $rows = [['<fg=cyan>Migration</>', $migrationPath]];

        if (file_exists($rollbackPath)) {
            $rows[] = ['<fg=cyan>Rollback</>', $rollbackPath];
        }

        $io->table(['File', 'Path'], $rows);

        return Command::SUCCESS;
    }
}
