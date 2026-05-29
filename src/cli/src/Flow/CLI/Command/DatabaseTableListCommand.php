<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Flow\CLI\Command\Traits\ConfigOptions;
use Flow\CLI\Command\Traits\DBOptions;
use RuntimeException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\TableSeparator;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function Flow\CLI\option_include_file;
use function Flow\CLI\option_list_of_strings_nullable;
use function Flow\Types\DSL\type_string;
use function in_array;

final class DatabaseTableListCommand extends Command
{
    use ConfigOptions;
    use DBOptions;

    private ?Connection $connection = null;

    public function configure(): void
    {
        $this
            ->setName('db:table:list')
            ->setDescription('Print list of datasets from a database.')
            ->setHelp(self::DB_CONNECTION_HELP)
            ->addOption(
                'db-namespace',
                null,
                InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY,
                'List of namespaces for which this command will list tables, multiple values allowed. When not set, all tables from all namespaces are listed.',
                [],
            );

        $this->addConfigOptions($this);
        $this->addDbOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ($this->connection === null) {
            throw new RuntimeException('Command not properly initialized.');
        }

        $style = new SymfonyStyle($input, $output);

        $table = $style->createTable();
        $table->setHeaders(['Name', 'Namespace', 'Columns'])->setStyle('box');

        $namespaces = option_list_of_strings_nullable('db-namespace', $input);

        $dbTables = [];
        $totalColumns = 0;

        foreach ($this->connection->createSchemaManager()->introspectTables() as $dbTable) {
            $qualifier = $dbTable->getObjectName()->getQualifier();

            $dbTables[] = [
                $dbTable->getObjectName()->getUnqualifiedName()->getValue(),
                $qualifier !== null ? $qualifier->getValue() : 'public',
                count($dbTable->getColumns()),
            ];
            $totalColumns += count($dbTable->getColumns());
        }

        if ($namespaces) {
            $dbTables = array_filter($dbTables, static fn(array $row) => in_array($row[1], $namespaces, true));
        }

        // order $rows by namespace, name
        usort($dbTables, static fn(array $a, array $b) => $a[1] === $b[1] ? $a[0] <=> $b[0] : $a[1] <=> $b[1]);

        $table->setRows($dbTables);
        $table->render();

        $style->definitionList(
            'Summary',
            new TableSeparator(),
            ['Total tables' => count($dbTables)],
            ['Total namespaces' => count(array_unique(array_column($dbTables, 1)))],
            ['Total columns' => $totalColumns],
        );

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output): void
    {
        if ($input->getOption('db-connection-file')) {
            $this->connection = option_include_file('db-connection-file', $input, Connection::class);
        } else {
            $style = new SymfonyStyle($input, $output);
            $connectionString = type_string()->assert(
                $_ENV['FLOW_DB_CONNECTION_STRING'] ?? $style->ask(
                    "FLOW_DB_CONNECTION_STRING env not found.\n Please provide database connection string, format:\n \"scheme://username:password@host:port/dbname?param1=value1&param2=value2&...\"",
                    null,
                    static fn($value) => $value,
                ),
            );

            $this->connection = DriverManager::getConnection((new DsnParser())->parse($connectionString));
        }
    }
}
