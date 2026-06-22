<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function realpath;
use function sprintf;

#[AsCommand(name: 'flow:migrations:generate', description: 'Generate a blank migration class')]
final class GenerateCommand extends Command
{
    public function __construct(
        private readonly MigrationGenerator $generator,
        private readonly MigrationsConfiguration $configuration,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('name', InputArgument::OPTIONAL, 'The name of the migration (e.g. \'add_email_index\')');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        /** @var ?string $name */
        $name = $input->getArgument('name');

        $version = $this->generator->generateDataMigration($name);

        $directory =
            $this->configuration->migrationsDirectory . '/' . (string) $version . ($name !== null ? '_' . $name : '');
        $realDirectory = realpath($directory) ?: $directory;

        $io->success(sprintf('Generated migration: %s', $version));

        $io->table(['File', 'Path'], [
            ['<fg=cyan>Migration</>', $realDirectory . '/' . $this->configuration->migrationFileName],
            ['<fg=cyan>Rollback</>', $realDirectory . '/' . $this->configuration->rollbackFileName],
        ]);

        return Command::SUCCESS;
    }
}
