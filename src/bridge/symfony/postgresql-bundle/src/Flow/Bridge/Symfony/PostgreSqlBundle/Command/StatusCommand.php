<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use function Flow\Types\DSL\{type_instance_of, type_string};

use Flow\PostgreSql\Migrations\Migrator;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use Symfony\Component\DependencyInjection\ContainerInterface;

#[AsCommand(name: 'flow:migrations:status', description: 'View the migration status')]
final class StatusCommand extends Command
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
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $migrator = type_instance_of(Migrator::class)->assert($this->container->get("flow.postgresql.{$connection}.migrations.migrator"));

        $status = $migrator->status();

        $total = \count($status);
        $executed = \count($status->executed());
        $pending = \count($status->pending());

        $io->title('Migration Status');

        $io->definitionList(
            ['Connection' => "<fg=cyan>{$connection}</>"],
            ['Total migrations' => (string) $total],
            ['Executed' => "<fg=green>{$executed}</>"],
            ['Pending' => $pending > 0 ? "<fg=yellow>{$pending}</>" : '<fg=green>0</>'],
        );

        if ($pending > 0) {
            $io->warning(\sprintf('%d migration(s) pending. Run <fg=yellow>flow:migrations:migrate</> to execute.', $pending));
        } else {
            $io->success('All migrations are up to date.');
        }

        return Command::SUCCESS;
    }
}
