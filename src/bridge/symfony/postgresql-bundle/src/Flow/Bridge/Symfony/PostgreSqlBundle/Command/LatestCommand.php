<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function array_slice;
use function count;
use function end;
use function iterator_to_array;
use function sprintf;

#[AsCommand(name: 'flow:migrations:latest', description: 'Output the latest available migration version')]
final class LatestCommand extends Command
{
    public function __construct(
        private readonly Migrator $migrator,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $statuses = $this->migrator->status();

        if (count($statuses) === 0) {
            $io->note('No migrations available.');

            return Command::SUCCESS;
        }

        $io->title('Latest Migrations');

        $all = iterator_to_array($statuses);
        $latest = array_slice($all, -5);

        $rows = [];

        foreach ($latest as $migration) {
            $isLast = $migration === end($all);
            $stateLabel = match ($migration->state) {
                MigrationState::EXECUTED => '<fg=green>EXECUTED</>',
                MigrationState::PENDING => '<fg=yellow>PENDING</>',
                MigrationState::UNAVAILABLE => '<fg=red>UNAVAILABLE</>',
            };

            $rows[] = [
                $isLast ? '<fg=cyan>►</>' : ' ',
                $stateLabel,
                (string) $migration->version,
                $migration->name,
            ];
        }

        $io->table(['', 'Status', 'Version', 'Name'], $rows);

        if (count($all) > 5) {
            $io->text(sprintf('  <fg=gray>Showing last 5 of %d migrations</>', count($all)));
            $io->newLine();
        }

        return Command::SUCCESS;
    }
}
