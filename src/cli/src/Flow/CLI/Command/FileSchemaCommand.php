<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{option_bool, option_int_nullable};
use function Flow\ETL\DSL\{df, schema_to_json};
use Flow\CLI\Arguments\{FilePathArgument};
use Flow\CLI\Command\Traits\{
    CSVOptions,
    ConfigOptions,
    JSONOptions,
    ParquetOptions,
    XMLOptions
};
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Options\{ConfigOption, FileFormat, FileFormatOption};
use Flow\ETL\Config;
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Schema\Formatter\{PHPSchemaFormatter};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class FileSchemaCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use JSONOptions;
    use ParquetOptions;
    use XMLOptions;

    private ?FileFormat $fileFormat = null;

    private ?Config $flowConfig = null;

    private ?Path $sourcePath = null;

    public function configure() : void
    {
        $this
            ->setName('file:schema')
            ->setDescription('Read and print (json by default) data schema from a file.')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a file from which schema should be extracted.')
            ->addOption('input-file-format', null, InputArgument::OPTIONAL, 'Source file format. When not set file format is guessed from source file path extension', null)
            ->addOption('input-file-limit', null, InputOption::VALUE_REQUIRED, 'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed', null)
            ->addOption('output-pretty', null, InputOption::VALUE_NONE, 'Print schema as pretty json')
            ->addOption('output-php', null, InputOption::VALUE_NONE, 'Print schema as PHP code')
            ->addOption('output-table', null, InputOption::VALUE_NONE, 'Print schema as ascii table')
            ->addOption('output-ascii', null, InputOption::VALUE_NONE, 'Print schema as ascii list')
            ->addOption('schema-auto-cast', null, InputOption::VALUE_OPTIONAL, 'When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type', false);

        $this->addConfigOptions($this);
        $this->addJSONInputOptions($this);
        $this->addCSVInputOptions($this);
        $this->addXMLInputOptions($this);
        $this->addParquetInputOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $df = df($this->flowConfig)->read((new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input));

        if (option_bool('schema-auto-cast', $input)) {
            $df->autoCast();
        }

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $schema = $df->schema();

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

        $style->writeln(schema_to_json($schema, option_bool('output-pretty', $input)));

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input, $this->flowConfig);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
