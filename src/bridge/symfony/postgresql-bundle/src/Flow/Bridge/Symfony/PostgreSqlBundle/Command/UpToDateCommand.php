<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Migrator;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function sprintf;

#[AsCommand(name: 'flow:migrations:up-to-date', description: 'Check if all migrations have been executed')]
final class UpToDateCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $migrator = type_instance_of(Migrator::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.migrator",
        ));

        $status = $migrator->status();
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
