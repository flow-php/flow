<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Options\ConfigOption;
use Flow\CLI\Options\CSVInputOptions;
use Flow\CLI\Options\ExcelInputOptions;
use Flow\CLI\Options\FileFormat;
use Flow\CLI\Options\FileFormatOption;
use Flow\CLI\Options\JSONInputOptions;
use Flow\CLI\Options\ParquetInputOptions;
use Flow\CLI\Options\SchemaInferenceOptions;
use Flow\CLI\Options\XMLInputOptions;
use Flow\ETL\IO\Formatter\AsciiTableFormatter;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Path;
use RuntimeException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function Flow\CLI\option_int;
use function Flow\CLI\option_int_nullable;
use function Flow\CLI\option_list_of_strings;
use function Flow\ETL\DSL\df;

#[AsCommand(name: 'file:read', description: 'Read data from a file.', aliases: ['read'])]
final class FileReadCommand extends Command
{
    private const int DEFAULT_BATCH_SIZE = 100;

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
                'File format. When not set file format is guessed from source file path extension',
                null,
            )
            ->addOption(
                'input-file-batch-size',
                null,
                InputOption::VALUE_REQUIRED,
                'Number of rows processed in one batch after reading, when set to -1 whole dataset will be displayed at once',
                self::DEFAULT_BATCH_SIZE,
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
            ->addOption(
                'output-truncate',
                null,
                InputOption::VALUE_REQUIRED,
                'Truncate output to given number of characters, when set to -1 output is not truncated at all',
                20,
            )
            ->addOption(
                'output-columns',
                null,
                InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY,
                'Columns to include in output, when not set all columns are displayed',
                [],
            );

        (new ConfigOption('config'))->configure($this);
        (new JSONInputOptions())->configure($this);
        (new CSVInputOptions())->configure($this);
        (new ExcelInputOptions())->configure($this);
        (new XMLInputOptions())->configure($this);
        (new ParquetInputOptions())->configure($this);
        (new SchemaInferenceOptions())->configure($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ($this->flowConfig === null || $this->sourcePath === null || $this->fileFormat === null) {
            throw new RuntimeException('Command not properly initialized.');
        }

        $style = new SymfonyStyle($input, $output);

        $extractor = (new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input);

        if (!(new SchemaInferenceOptions())->apply($extractor, $input, $style)) {
            return Command::FAILURE;
        }

        $df = df($this->flowConfig)->read($extractor);

        $batchSize = option_int('input-file-batch-size', $input, self::DEFAULT_BATCH_SIZE);
        $outputTruncate = option_int('output-truncate', $input, 20);

        if ($batchSize <= 0) {
            $style->error('Batch size must be greater than 0.');

            return Command::FAILURE;
        }

        $df = $df->batchSize($batchSize);

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df = $df->limit($limit);
        }

        $offset = option_int_nullable('input-file-offset', $input);

        if ($offset !== null && $offset > 0) {
            $df = $df->offset($offset);
        }

        $outputColumns = option_list_of_strings('output-columns', $input);

        if (count($outputColumns)) {
            $df = $df->select(...$outputColumns);
        }

        $formatter = new AsciiTableFormatter();

        $df->forEach(static function (Rows $rows) use ($style, $formatter, $outputTruncate): void {
            $style->write($formatter->format($rows, $outputTruncate));
        });

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output): void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
