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

use function count;
use function sprintf;

#[AsCommand(name: 'flow:migrations:list', description: 'List all available migrations')]
final class ListCommand extends Command
{
    public function __construct(
        private readonly Migrator $migrator,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Migrations');

        $statuses = $this->migrator->status();

        if (count($statuses) === 0) {
            $io->note('No migrations found.');

            return Command::SUCCESS;
        }

        $rows = [];

        foreach ($statuses as $migration) {
            $stateLabel = match ($migration->state) {
                MigrationState::EXECUTED => '<fg=green>EXECUTED</>',
                MigrationState::PENDING => '<fg=yellow>PENDING</>',
                MigrationState::UNAVAILABLE => '<fg=red>UNAVAILABLE</>',
            };

            $executedAt = $migration->executedAt !== null
                ? '<fg=gray>' . $migration->executedAt->format('Y-m-d H:i:s') . '</>'
                : '<fg=gray>-</>';

            $rows[] = [$stateLabel, (string) $migration->version, $migration->name, $executedAt];
        }

        $io->table(['Status', 'Version', 'Name', 'Executed at'], $rows);

        $executed = count($statuses->executed());
        $pending = count($statuses->pending());

        $io->text(sprintf(
            '  <fg=green>%d</> executed, <fg=%s>%d</> pending',
            $executed,
            $pending > 0 ? 'yellow' : 'green',
            $pending,
        ));
        $io->newLine();

        return Command::SUCCESS;
    }
}
