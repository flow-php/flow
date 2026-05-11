<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

#[AsCommand(name: 'flow:migrations:list', description: 'List all available migrations')]
final class ListCommand extends Command
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

        $io->title('Migrations');

        $statuses = $migrator->status();

        if (\count($statuses) === 0) {
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

        $executed = \count($statuses->executed());
        $pending = \count($statuses->pending());

        $io->text(\sprintf(
            '  <fg=green>%d</> executed, <fg=%s>%d</> pending',
            $executed,
            $pending > 0 ? 'yellow' : 'green',
            $pending,
        ));
        $io->newLine();

        return Command::SUCCESS;
    }
}
