<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Command\Traits\ConfigOptions;
use Flow\CLI\Command\Traits\CSVOptions;
use Flow\CLI\Command\Traits\ExcelOptions;
use Flow\CLI\Command\Traits\JSONOptions;
use Flow\CLI\Command\Traits\ParquetOptions;
use Flow\CLI\Command\Traits\SchemaInferenceOptions;
use Flow\CLI\Command\Traits\XMLOptions;
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Options\ConfigOption;
use Flow\CLI\Options\FileFormat;
use Flow\CLI\Options\FileFormatOption;
use Flow\ETL\Config;
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Schema\Formatter\PHPSchemaFormatter;
use Flow\Filesystem\Path;
use RuntimeException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\CLI\option_bool;
use function Flow\CLI\option_int_nullable;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\schema_to_json;

#[AsCommand(
    name: 'file:schema',
    description: 'Read and print (json by default) data schema from a file.',
    aliases: ['schema'],
)]
final class FileSchemaCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use ExcelOptions;
    use JSONOptions;
    use ParquetOptions;
    use SchemaInferenceOptions;
    use XMLOptions;

    private ?FileFormat $fileFormat = null;

    private ?Config $flowConfig = null;

    private ?Path $sourcePath = null;

    public function configure(): void
    {
        $this
            ->addArgument(
                'input-file',
                InputArgument::REQUIRED,
                'Path to a file from which schema should be extracted.',
            )
            ->addOption(
                'input-file-format',
                null,
                InputArgument::OPTIONAL,
                'Source file format. When not set file format is guessed from source file path extension',
                null,
            )
            ->addOption(
                'input-file-limit',
                null,
                InputOption::VALUE_REQUIRED,
                'Limit number of rows read from the file.',
                null,
            )
            ->addOption(
                'input-file-offset',
                null,
                InputOption::VALUE_REQUIRED,
                'Number of rows to skip before starting to read data',
                null,
            )
            ->addOption('output-pretty', null, InputOption::VALUE_NONE, 'Print schema as pretty json')
            ->addOption('output-php', null, InputOption::VALUE_NONE, 'Print schema as PHP code')
            ->addOption('output-table', null, InputOption::VALUE_NONE, 'Print schema as ascii table')
            ->addOption('output-ascii', null, InputOption::VALUE_NONE, 'Print schema as ascii list');

        $this->addConfigOptions($this);
        $this->addJSONInputOptions($this);
        $this->addCSVInputOptions($this);
        $this->addExcelInputOptions($this);
        $this->addXMLInputOptions($this);
        $this->addParquetInputOptions($this);
        $this->addSchemaInferenceOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ($this->flowConfig === null || $this->sourcePath === null || $this->fileFormat === null) {
            throw new RuntimeException('Command not properly initialized.');
        }

        $style = new SymfonyStyle($input, $output);

        $extractor = (new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input);

        if (!$this->applySchemaInference($extractor, $input, $style)) {
            return Command::FAILURE;
        }

        $df = df($this->flowConfig)->read($extractor);

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $offset = option_int_nullable('input-file-offset', $input);

        if ($offset !== null && $offset > 0) {
            $df->offset($offset);
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

    protected function initialize(InputInterface $input, OutputInterface $output): void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
