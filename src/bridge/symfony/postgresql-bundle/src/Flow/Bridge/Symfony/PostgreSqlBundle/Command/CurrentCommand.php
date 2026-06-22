<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;

#[AsCommand(name: 'flow:migrations:current', description: 'Output the current migration version')]
final class CurrentCommand extends Command
{
    public function __construct(
        private readonly MigrationStore $store,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $this->store->initialize();
        $executed = $this->store->executedMigrations();
        $latest = $executed->latest();

        $io->title('Current Migration');

        if ($latest === null) {
            $io->note('No migrations have been executed yet.');

            return Command::SUCCESS;
        }

        $io->definitionList(
            ['Version' => "<fg=cyan>{$latest->version}</>"],
            ['Executed at' => '<fg=gray>' . $latest->executedAt->format('Y-m-d H:i:s') . '</>'],
            ['Execution time' => $latest->executionTimeMs !== null ? $latest->executionTimeMs . 'ms' : '<fg=gray>-</>'],
            ['Total executed' => (string) count($executed)],
        );

        return Command::SUCCESS;
    }
}
