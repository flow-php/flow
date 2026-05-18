<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Client\Client;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use function array_keys;
use function array_map;
use function count;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function implode;
use function sprintf;
use function str_repeat;
use function str_starts_with;
use function strtoupper;
use function strval;
use function trim;

#[AsCommand(name: 'flow:sql:run', description: 'Execute SQL directly on the database')]
final class RunSqlCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)->addArgument(
            'sql',
            InputArgument::REQUIRED,
            'The SQL statement to execute',
        );
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $client = type_instance_of(Client::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.client",
        ));
        $sql = type_string()->assert($input->getArgument('sql'));

        if (str_starts_with(strtoupper(trim($sql)), 'SELECT')) {
            $rows = $client->fetchAll($sql);

            if (empty($rows)) {
                $output->writeln('<info>No results.</info>');

                return Command::SUCCESS;
            }

            $headers = array_keys($rows[0]);
            $output->writeln(implode("\t", $headers));
            $output->writeln(str_repeat('-', 80));

            foreach ($rows as $row) {
                /** @var array<string, null|scalar> $row */
                $output->writeln(implode("\t", array_map(static fn($v) => $v === null ? 'NULL' : strval($v), $row)));
            }

            $output->writeln('');
            $output->writeln(sprintf('<info>%d row(s) returned.</info>', count($rows)));

            return Command::SUCCESS;
        }

        $affected = $client->execute($sql);
        $output->writeln(sprintf('<info>%d row(s) affected.</info>', $affected));

        return Command::SUCCESS;
    }
}
