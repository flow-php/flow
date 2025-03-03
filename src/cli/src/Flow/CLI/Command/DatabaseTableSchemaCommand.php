<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{argument_string_nullable, option_bool, option_include_file};
use function Flow\ETL\Adapter\Doctrine\table_schema_to_flow_schema;
use function Flow\ETL\DSL\schema_to_json;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\DBAL\{Connection, DriverManager};
use Flow\CLI\Command\Traits\{ConfigOptions, DBOptions};
use Flow\CLI\Options\{ConfigOption};
use Flow\ETL\Config;
use Flow\ETL\Row\Schema\Formatter\{ASCIISchemaFormatter, PHPSchemaFormatter};
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Question\{ChoiceQuestion};
use Symfony\Component\Console\Style\SymfonyStyle;

final class DatabaseTableSchemaCommand extends Command
{
    use ConfigOptions;
    use DBOptions;

    private ?Connection $connection = null;

    private ?Config $flowConfig = null;

    public function configure() : void
    {
        $this
            ->setName('db:table:schema')
            ->setDescription('Read data schema from a database table.')
            ->setHelp(self::DB_CONNECTION_HELP)
            ->addArgument('input-db-table', InputArgument::OPTIONAL, 'Table name for which we are going to generate schema.')
            ->addOption('output-php', null, InputOption::VALUE_NONE, 'Print schema as PHP code')
            ->addOption('output-table', null, InputOption::VALUE_NONE, 'Print schema as ascii table')
            ->addOption('output-ascii', null, InputOption::VALUE_NONE, 'Print schema as ascii list');

        $this->addConfigOptions($this);
        $this->addDbOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $tableName = argument_string_nullable('input-db-table', $input);

        if (!$tableName) {
            $question = new ChoiceQuestion(
                'Please select table name for which we are going to generate schema: ',
                $this->connection->createSchemaManager()->listTableNames()
            );
            $question->setErrorMessage('Invalid table: %s');
            $tableName = $style->askQuestion($question);
        }

        $table = null;

        foreach ($this->connection->createSchemaManager()->listTables() as $dbTable) {
            if ($dbTable->getName() === $tableName) {
                $table = $dbTable;

                break;
            }
        }

        if (!$table) {
            $style->error("Table {$tableName} not found.");

            return Command::FAILURE;
        }

        $schema = table_schema_to_flow_schema($table);

        if (option_bool('output-ascii', $input)) {
            $style->write((new ASCIISchemaFormatter())->format($schema));

            return Command::SUCCESS;
        }

        if (option_bool('output-table', $input)) {
            $style->write((new ASCIISchemaFormatter(true))->format($schema));

            return Command::SUCCESS;
        }

        if (option_bool('output-php', $input)) {
            $style->writeln((new PHPSchemaFormatter())->format($schema));

            return Command::SUCCESS;
        }

        $style->writeln(schema_to_json($schema, true));

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);

        if ($input->getOption('db-connection-file')) {
            $this->connection = option_include_file('db-connection-file', $input, Connection::class);
        } else {
            $style = new SymfonyStyle($input, $output);
            $connectionString = $_ENV['FLOW_DB_CONNECTION_STRING']
                ?? $style->ask(
                    "FLOW_DB_CONNECTION_STRING env not found.\n Please provide database connection string, format:\n \"scheme://username:password@host:port/dbname?param1=value1&param2=value2&...\"",
                    null,
                    fn ($value) => $value
                );
            $connectionParameters = (new DsnParser())->parse($connectionString);
            $this->connection = DriverManager::getConnection($connectionParameters);
        }
    }
}
