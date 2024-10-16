<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{option_bool, option_int, option_int_nullable};
use function Flow\ETL\DSL\{df};
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
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\{Config, Rows};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class FileReadCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use JSONOptions;
    use ParquetOptions;
    use XMLOptions;

    private const DEFAULT_BATCH_SIZE = 100;

    private ?FileFormat $fileFormat = null;

    private ?Config $flowConfig = null;

    private ?Path $sourcePath = null;

    public function configure() : void
    {
        $this
            ->setName('file:read')
            ->setDescription('Read data from a file.')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a file from which schema should be extracted.')
            ->addOption('input-file-format', null, InputArgument::OPTIONAL, 'File format. When not set file format is guessed from source file path extension', null)
            ->addOption('input-file-batch-size', null, InputOption::VALUE_REQUIRED, 'Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once', self::DEFAULT_BATCH_SIZE)
            ->addOption('input-file-limit', null, InputOption::VALUE_REQUIRED, 'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed', null)
            ->addOption('output-truncate', null, InputOption::VALUE_REQUIRED, 'Truncate output to given number of characters, when set to -1 output is not truncated at all', 20)
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

        $batchSize = option_int('input-file-batch-size', $input, self::DEFAULT_BATCH_SIZE);
        $outputTruncate = option_int('output-truncate', $input, 20);

        if ($batchSize <= 0) {
            $style->error('Batch size must be greater than 0.');

            return Command::FAILURE;
        }

        $df->batchSize($batchSize);

        if (option_bool('schema-auto-cast', $input)) {
            $df->autoCast();
        }

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $formatter = new AsciiTableFormatter();

        $df->run(function (Rows $rows) use ($style, $formatter, $outputTruncate) : void {
            $style->write($formatter->format($rows, $outputTruncate));
        });

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input, $this->flowConfig);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
