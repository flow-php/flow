<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{config_builder, df, from_array, ref, schema_to_json, to_output};
use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\Config;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class FileSchemaCommand extends Command
{
    private ?Config $flowConfig;

    private ?string $inputFormat = null;

    private ?Path $sourcePath = null;

    public function configure() : void
    {
        $this
            ->setName('file:schema')
            ->setDescription('Read data schema from a file.')
            ->addArgument('source', InputArgument::REQUIRED, 'Path to a file from which schema should be extracted.')
            ->addOption('input-format', 'if', InputArgument::OPTIONAL, 'Source file format. When not set file format is guessed from source file path extension', null)
            ->addOption('pretty', null, InputOption::VALUE_OPTIONAL, 'Pretty print schema', false)
            ->addOption('table', null, InputOption::VALUE_OPTIONAL, 'Pretty schema as ascii table', false)
            ->addOption('auto-cast', null, InputOption::VALUE_OPTIONAL, 'When set Flow will try to automatically cast values to more precise data types, for example datetime strings will be casted to datetime type', false);
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $autoCast = ($input->getOption('auto-cast') !== false);

        $df = df($this->flowConfig)
            ->read(match ($this->inputFormat) {
                'csv' => from_csv($this->sourcePath),
                'json' => from_json($this->sourcePath),
                'xml' => from_xml($this->sourcePath),
                'parquet' => from_parquet($this->sourcePath),
            });

        if ($autoCast) {
            $df->autoCast();
        }

        $schema = $df->schema();

        $prettyValue = $input->getOption('pretty');
        $prettyPrint = ($prettyValue !== false);

        $tableValue = $input->getOption('table');
        $tablePrint = ($tableValue !== false);

        if ($tablePrint) {
            ob_start();
            df()
                ->read(from_array($schema->normalize()))
                ->withEntry('type', ref('type')->unpack())
                ->renameAll('type.', '')
                ->rename('ref', 'name')
                ->collect()
                ->select('name', 'type', 'nullable', 'scalar_type', 'metadata')
                ->write(to_output())
                ->run();

            $style->write(ob_get_clean());

            return Command::SUCCESS;
        }

        $style->writeln(schema_to_json($schema, $prettyPrint ? JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR : JSON_THROW_ON_ERROR));

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = config_builder()->build();

        $source = (string) $input->getArgument('source');

        $sourcePath = path_real($source);

        $fs = $this->flowConfig->fstab()->for($sourcePath);

        if (!$fs->status($sourcePath)) {
            throw new InvalidArgumentException(\sprintf('File "%s" does not exist.', $sourcePath->path()));
        }

        $supportedFormats = ['csv', 'json', 'xml', 'parquet', 'txt'];

        $inputFormat = \mb_strtolower($input->getOption('input-format') ?: $sourcePath->extension());

        if (!\in_array($inputFormat, $supportedFormats, true)) {
            throw new InvalidArgumentException(\sprintf('File format "%s" is not supported. Input file format can be set with --input-format option', $inputFormat));
        }

        $this->sourcePath = $sourcePath;
        $this->inputFormat = $inputFormat;
    }
}
