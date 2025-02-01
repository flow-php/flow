<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{option_int, option_int_nullable};
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
use Flow\CLI\Style\FlowStyle;
use Flow\ETL\{Config, Rows};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;

final class FileAnalyzeCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use JSONOptions;
    use ParquetOptions;
    use XMLOptions;

    private const DEFAULT_BATCH_SIZE = 1_000;

    private ?FileFormat $fileFormat = null;

    private ?Config $flowConfig = null;

    private ?Path $sourcePath = null;

    public function configure() : void
    {
        $this
            ->setName('file:analyze')
            ->setDescription('Analyze a file.')
            ->addArgument('input-file', InputArgument::REQUIRED, 'Path to a file from which schema should be extracted.')
            ->addOption('input-file-format', null, InputArgument::OPTIONAL, 'File format. When not set file format is guessed from source file path extension', null)
            ->addOption('input-file-batch-size', null, InputOption::VALUE_REQUIRED, 'Number of rows that are going to be read and displayed in one batch, when set to -1 whole dataset will be displayed at once', self::DEFAULT_BATCH_SIZE)
            ->addOption('input-file-limit', null, InputOption::VALUE_REQUIRED, 'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed', null);

        $this->addConfigOptions($this);
        $this->addJSONInputOptions($this);
        $this->addCSVInputOptions($this);
        $this->addXMLInputOptions($this);
        $this->addParquetInputOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new FlowStyle($input, $output);

        $style->title('Analyzing File');
        $style->info('File path: ' . $this->sourcePath->basename());

        $df = df($this->flowConfig)->read((new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input));

        $batchSize = option_int('input-file-batch-size', $input, self::DEFAULT_BATCH_SIZE);

        if ($batchSize <= 0) {
            $style->error('Batch size must be greater than 0.');

            return Command::FAILURE;
        }

        $df->batchSize($batchSize)
            ->autoCast();

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $progress = $style->createProgressBar();
        $progress->setFormat('Analyzed Rows: %current% %bar%');

        $report = $df->run(
            static function (Rows $rows) use ($progress) : void {
                $progress->advance($rows->count());
            },
            analyze: true
        );

        if ($report === null) {
            $style->error("Couldn't analyze given file.");

            return Command::FAILURE;
        }

        $progress->finish();

        $style->newLine(2);

        $style->clear();

        $style->section('Schema');

        $normalizedSchema = [];

        foreach ($report->schema()->definitions() as $definition) {
            $normalizedSchema[] = [
                'name' => $definition->entry()->name(),
                'type' => $definition->type()->toString(),
                'nullable' => $definition->isNullable() ? 'true' : 'false',
                'metadata' => $definition->metadata() !== null ? json_encode($definition->metadata(), JSON_PRETTY_PRINT) : null,
            ];
        }

        $style->createTable()
            ->setHeaders(['Name', 'Type', 'Nullable', 'Metadata'])
            ->setRows($normalizedSchema)
            ->setStyle('box')
            ->render();

        $formatter = $this->getHelper('formatter');

        $style->section('Statistics');

        $output->writeln(
            $formatter->formatBlock(
                [
                    'Analyzed Rows: ' . \number_format($report->statistics()->totalRows()),
                ],
                'blue-block',
                true
            )
        );

        $output->writeln(
            $formatter->formatBlock(
                [
                    'Execution Time: ' . $report->statistics()->executionTime->highResolutionTime->toString(),
                ],
                'blue-block',
                true
            )
        );

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input, $this->flowConfig);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
