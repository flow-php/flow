<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Migrator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function sprintf;

#[AsCommand(name: 'flow:migrations:up-to-date', description: 'Check if all migrations have been executed')]
final class UpToDateCommand extends Command
{
    public function __construct(
        private readonly Migrator $migrator,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $status = $this->migrator->status();
        $pending = $status->pending();

        if (count($pending) === 0) {
            $io->success('All migrations are up to date.');

            return Command::SUCCESS;
        }

        $rows = [];

        foreach ($pending as $migration) {
            $rows[] = ['<fg=yellow>PENDING</>', (string) $migration->version, $migration->name];
        }

        $io->error(sprintf('Out of date! %d pending migration(s).', count($pending)));
        $io->table(['Status', 'Version', 'Name'], $rows);
        $io->text('  Run <fg=yellow>flow:migrations:migrate</> to execute pending migrations.');
        $io->newLine();

        return Command::FAILURE;
    }
}
