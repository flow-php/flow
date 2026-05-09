<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use function Flow\Types\DSL\{type_instance_of, type_string};

use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;

use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:migrations:current', description: 'Output the current migration version')]
final class CurrentCommand extends Command
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
        $store = type_instance_of(MigrationStore::class)->assert($this->container->get("flow.postgresql.{$connection}.migrations.store"));

        $store->initialize();
        $executed = $store->executedMigrations();
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
            ['Total executed' => (string) \count($executed)],
        );

        return Command::SUCCESS;
    }
}
