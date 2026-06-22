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

#[AsCommand(name: 'flow:migrations:status', description: 'View the migration status')]
final class StatusCommand extends Command
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

        $total = count($status);
        $executed = count($status->executed());
        $pending = count($status->pending());

        $io->title('Migration Status');

        $io->definitionList(
            ['Total migrations' => (string) $total],
            ['Executed' => "<fg=green>{$executed}</>"],
            ['Pending' => $pending > 0 ? "<fg=yellow>{$pending}</>" : '<fg=green>0</>'],
        );

        if ($pending > 0) {
            $io->warning(sprintf(
                '%d migration(s) pending. Run <fg=yellow>flow:migrations:migrate</> to execute.',
                $pending,
            ));
        } else {
            $io->success('All migrations are up to date.');
        }

        return Command::SUCCESS;
    }
}
