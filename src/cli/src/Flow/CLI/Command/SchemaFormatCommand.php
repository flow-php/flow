<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\option_bool;
use function Flow\ETL\DSL\{schema_from_json, schema_to_json};
use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Command\Traits\ConfigOptions;
use Flow\CLI\Options\ConfigOption;
use Flow\ETL\{Config, Row\Formatter\ASCIISchemaFormatter, Schema\Formatter\PHPSchemaFormatter};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class SchemaFormatCommand extends Command
{
    use ConfigOptions;

    private ?Config $flowConfig = null;

    private ?Path $schemaPath = null;

    public function configure() : void
    {
        $this
            ->setName('schema:format')
            ->setDescription('Print a json schema in one of the available formats.')
            ->addArgument('input-schema-file', InputArgument::REQUIRED, 'Path to a json with schema Flow.')
            ->addOption('output-php', null, InputOption::VALUE_NONE, 'Print schema as PHP code')
            ->addOption('output-table', null, InputOption::VALUE_NONE, 'Print schema as ascii table')
            ->addOption('output-ascii', null, InputOption::VALUE_NONE, 'Print schema as ascii list');

        $this->addConfigOptions($this);
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $schema = schema_from_json($this->flowConfig->fstab()->for($this->schemaPath)->readFrom($this->schemaPath)->content());

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
        $this->schemaPath = (new FilePathArgument('input-schema-file'))->getExisting($input, $this->flowConfig);
    }
}
