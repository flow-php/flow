<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\{option_int_nullable};
use function Flow\ETL\DSL\{df};
use Flow\CLI\Arguments\{FilePathArgument};
use Flow\CLI\Command\Traits\{
    CSVExtractorOptions,
    ConfigOptions,
    JSONExtractorOptions,
    ParquetExtractorOptions,
    XMLExtractorOptions
};
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Options\{ConfigOption, FileFormat, FileFormatOption};
use Flow\ETL\Config;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class FileRowsCountCommand extends Command
{
    use ConfigOptions;
    use CSVExtractorOptions;
    use JSONExtractorOptions;
    use ParquetExtractorOptions;
    use XMLExtractorOptions;

    private ?FileFormat $fileFormat = null;

    private ?Config $flowConfig = null;

    private ?Path $sourcePath = null;

    public function configure() : void
    {
        $this
            ->setName('file:schema')
            ->setDescription('Read data schema from a file.')
            ->addArgument('file', InputArgument::REQUIRED, 'Path to a file from which schema should be extracted.')
            ->addOption('file-format', null, InputArgument::OPTIONAL, 'Source file format. When not set file format is guessed from source file path extension', null)
            ->addOption('file-limit', null, InputOption::VALUE_REQUIRED, 'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed', null);

        $this->addConfigOptions($this);
        $this->addJSONOptions($this);
        $this->addCSVOptions($this);
        $this->addXMLOptions($this);
        $this->addParquetOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $df = df($this->flowConfig)->read((new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input));

        $limit = option_int_nullable('file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $style->write((string) $df->count());

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('file'))->getExisting($input, $this->flowConfig);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'file-format'))->get($input);
    }
}
