<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use function Flow\PostgreSql\DSL\{agg_count, and_, col, drop, eq, func, ne, param, select};

use function Flow\Types\DSL\{type_instance_of, type_string};
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputInterface, InputOption};

use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: 'flow:database:drop', description: 'Drop the configured database')]
final class DropDatabaseCommand extends Command
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
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)
            ->addOption('if-exists', null, InputOption::VALUE_NONE, 'Don\'t error if database doesn\'t exist')
            ->addOption('force', null, InputOption::VALUE_NONE, 'Required to actually execute the drop');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);

        if (!$input->getOption('force')) {
            $output->writeln('<error>This operation cannot be undone. Use --force to proceed.</error>');

            return Command::FAILURE;
        }

        $params = type_instance_of(ConnectionParameters::class)->assert($this->container->get("flow.postgresql.{$connection}.connection_parameters"));

        $maintenanceClient = PgSqlClient::connect($params->withDatabase('postgres'));

        $exists = $maintenanceClient->fetchScalarInt(
            select(agg_count())->from('pg_database')->where(eq(col('datname'), param(1))),
            [$params->database()]
        ) > 0;

        if (!$exists) {
            $maintenanceClient->close();

            if ($input->getOption('if-exists')) {
                $output->writeln(\sprintf('<info>Database "%s" does not exist.</info>', $params->database()));

                return Command::SUCCESS;
            }

            $output->writeln(\sprintf('<error>Database "%s" does not exist.</error>', $params->database()));

            return Command::FAILURE;
        }

        $maintenanceClient->execute(
            select(func('pg_terminate_backend', [col('pid')]))
                ->from('pg_stat_activity')
                ->where(and_(eq(col('datname'), param(1)), ne(col('pid'), func('pg_backend_pid')))),
            [$params->database()]
        );
        $maintenanceClient->execute(drop()->database($params->database()));
        $maintenanceClient->close();

        $output->writeln(\sprintf('<info>Database "%s" dropped successfully.</info>', $params->database()));

        return Command::SUCCESS;
    }
}
