<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Command\Traits\ConfigOptions;
use Flow\CLI\Command\Traits\CSVOptions;
use Flow\CLI\Command\Traits\ExcelOptions;
use Flow\CLI\Command\Traits\JSONOptions;
use Flow\CLI\Command\Traits\ParquetOptions;
use Flow\CLI\Command\Traits\XMLOptions;
use Flow\CLI\Factory\ExtractorFactory;
use Flow\CLI\Options\ConfigOption;
use Flow\CLI\Options\FileFormat;
use Flow\CLI\Options\FileFormatOption;
use Flow\ETL\Config;
use Flow\Filesystem\Path;
use RuntimeException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\CLI\option_int_nullable;
use function Flow\ETL\DSL\df;

#[AsCommand(name: 'file:rows:count', description: 'Count rows in a file.', aliases: ['count'])]
final class FileRowsCountCommand extends Command
{
    use ConfigOptions;
    use CSVOptions;
    use ExcelOptions;
    use JSONOptions;
    use ParquetOptions;
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
                'Limit number of rows that are going to be used to infer file schema, when not set whole file is analyzed',
                null,
            )
            ->addOption(
                'input-file-offset',
                null,
                InputOption::VALUE_REQUIRED,
                'Number of rows to skip before starting to read data',
                null,
            );

        $this->addConfigOptions($this);
        $this->addJSONInputOptions($this);
        $this->addExcelInputOptions($this);
        $this->addCSVInputOptions($this);
        $this->addXMLInputOptions($this);
        $this->addParquetInputOptions($this);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        if ($this->flowConfig === null || $this->sourcePath === null || $this->fileFormat === null) {
            throw new RuntimeException('Command not properly initialized.');
        }

        $style = new SymfonyStyle($input, $output);

        $df = df($this->flowConfig)->read((new ExtractorFactory($this->sourcePath, $this->fileFormat))->get($input));

        $limit = option_int_nullable('input-file-limit', $input);

        if ($limit !== null && $limit > 0) {
            $df->limit($limit);
        }

        $offset = option_int_nullable('input-file-offset', $input);

        if ($offset !== null && $offset > 0) {
            $df->offset($offset);
        }

        $style->write((string) $df->count());

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output): void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->sourcePath = (new FilePathArgument('input-file'))->getExisting($input);
        $this->fileFormat = (new FileFormatOption($this->sourcePath, 'input-file-format'))->get($input);
    }
}
