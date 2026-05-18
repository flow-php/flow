<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Infrastructure\PgSql\PgSqlClient;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\select;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function sprintf;

#[AsCommand(name: 'flow:database:create', description: 'Create the configured database')]
final class CreateDatabaseCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)->addOption(
            'if-not-exists',
            null,
            InputOption::VALUE_NONE,
            'Don\'t error if database already exists',
        );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $params = type_instance_of(ConnectionParameters::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.connection_parameters",
        ));

        $maintenanceClient = PgSqlClient::connect($params->withDatabase('postgres'));

        $exists =
            $maintenanceClient->fetchScalarInt(
                select(agg_count())->from('pg_database')->where(eq(col('datname'), param(1))),
                [$params->database()],
            ) > 0;

        if ($exists) {
            $maintenanceClient->close();

            if ($input->getOption('if-not-exists')) {
                $output->writeln(sprintf('<info>Database "%s" already exists.</info>', $params->database()));

                return Command::SUCCESS;
            }

            $output->writeln(sprintf('<error>Database "%s" already exists.</error>', $params->database()));

            return Command::FAILURE;
        }

        $maintenanceClient->execute(create()->database($params->database()));
        $maintenanceClient->close();

        $output->writeln(sprintf('<info>Database "%s" created successfully.</info>', $params->database()));

        return Command::SUCCESS;
    }
}
