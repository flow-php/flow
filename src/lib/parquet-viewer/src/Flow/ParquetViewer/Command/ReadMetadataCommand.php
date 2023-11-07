<?php declare(strict_types=1);

namespace Flow\ParquetViewer\Command;

use Coduo\PHPHumanizer\StringHumanizer;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\TableCell;
use Symfony\Component\Console\Helper\TableSeparator;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'read:metadata', description: 'Read metadata from parquet file')]
final class ReadMetadataCommand extends Command
{
    protected function configure() : void
    {
        $this
            ->addArgument('file', InputArgument::REQUIRED, 'path to parquet file')
            ->addOption('columns', null, InputOption::VALUE_NONE, 'Display column details')
            ->addOption('row-groups', null, InputOption::VALUE_NONE, 'Display row group details')
            ->addOption('column-chunks', null, InputOption::VALUE_NONE, 'Display column chunks details')
            ->addOption('statistics', null, InputOption::VALUE_NONE, 'Display column chunks statistics details')
            ->addOption('page-headers', null, InputOption::VALUE_NONE, 'Display page headers details');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);
        $filePath = $input->getArgument('file');

        if (!\file_exists($filePath)) {
            $style->error(\sprintf('File "%s" does not exist', $filePath));

            return Command::FAILURE;
        }

        $displayColumns = (bool) $input->getOption('columns');
        $displayRowGroups = (bool) $input->getOption('row-groups');
        $displayStatistics = (bool) $input->getOption('statistics');
        $displayColumnChunks = (bool) $input->getOption('column-chunks');
        $displayPageHeaders = (bool) $input->getOption('page-headers');

        $reader = new Reader();
        $parquetFile = $reader->read($filePath);

        try {
            $metadata = $parquetFile->metadata();
        } catch (InvalidArgumentException $e) {
            $style->error(\sprintf('File "%s" is not a valid parquet file', $filePath));

            return Command::FAILURE;
        }

        $metadataTable = $style->createTable();

        $metadataTable->setHeaderTitle('Metadata');
        $metadataTable->setStyle('box');
        $metadataTable->setHorizontal();
        $metadataTable->setHeaders(['file path', 'parquet version', 'created by', 'rows']);
        $metadataTable->setRows([
            [
                \realpath($filePath),
                $metadata->version(),
                $metadata->createdBy(),
                \number_format($metadata->rowsNumber(), 0, '.', ','),
            ],
        ]);

        $metadataTable->render();
        $style->newLine();

        if ($displayColumns) {
            $columnsTable = $style->createTable();
            $columnsTable->setStyle('box');
            $columnsTable->setHeaderTitle('Flat Columns');
            $columnsTable->setHeaders(['path', 'type', 'logical type', 'repetition', 'max repetition', 'max definition']);

            foreach ($parquetFile->schema()->columnsFlat() as $column) {
                $columnsTable->addRow([
                    $column->flatPath(),
                    $column->type() ? $column->type()->name : 'group',
                    $column->logicalType() ? $column->logicalType()->name() : '-',
                    $column->repetition()->name,
                    $column->maxRepetitionsLevel(),
                    $column->maxDefinitionsLevel(),
                ]);
            }

            $columnsTable->render();
            $style->newLine();
        }

        if ($displayRowGroups) {
            $rowGroupsTable = $style->createTable();
            $rowGroupsTable->setStyle('box');
            $rowGroupsTable->setHeaderTitle('Row Groups');
            $rowGroupsTable->setHeaders(['num rows', 'total byte size', 'columns count']);
            $totalRowGroups = 0;

            foreach ($metadata->rowGroups()->all() as $rowGroup) {
                $totalRowGroups++;
                $rowGroupsTable->addRow([
                    \number_format($rowGroup->rowsCount()),
                    \number_format($rowGroup->totalByteSize()),
                    \count($rowGroup->columnChunks()),
                ]);
            }

            $rowGroupsTable->setFooterTitle('Total: ' . \number_format($totalRowGroups));
            $rowGroupsTable->render();
            $style->newLine();
        }

        if ($displayColumnChunks) {
            $chunksTable = $style->createTable();
            $chunksTable->setStyle('box');
            $chunksTable->setHeaderTitle('Column Chunks');
            $chunksTable->setHeaders(['path', 'encodings', 'compression', 'file offset', 'num values', 'dictionary page offset', 'data page offset']);
            $totalChunks = 0;

            foreach ($metadata->rowGroups()->all() as $rowGroup) {
                foreach ($rowGroup->columnChunks() as $columnChunk) {
                    $totalChunks++;
                    $chunksTable->addRow([
                        $columnChunk->flatPath(),
                        '[' . \implode(',', \array_map(static fn ($e) => $e->name, $columnChunk->encodings())) . ']',
                        $columnChunk->codec()->name,
                        \number_format($columnChunk->fileOffset()),
                        \number_format($columnChunk->valuesCount()),
                        $columnChunk->dictionaryPageOffset() ? \number_format($columnChunk->dictionaryPageOffset()) : '-',
                        $columnChunk->dataPageOffset() ? \number_format($columnChunk->dataPageOffset()) : '-',
                    ]);

                }
            }
            $chunksTable->setFooterTitle('Total: ' . \number_format($totalChunks));
            $chunksTable->render();
            $style->newLine();
        }

        if ($displayStatistics) {
            $statisticsTable = $style->createTable();
            $statisticsTable->setStyle('box');
            $statisticsTable->setHeaderTitle('Column Chunks Statistics');
            $statisticsTable->setHeaders(['path', 'min [deprecated]', 'max [deprecated]', 'min value', 'max value', 'null count', 'distinct count']);
            $totalChunks = 0;

            foreach ($metadata->rowGroups()->all() as $rowGroupIndex => $rowGroup) {
                if ($totalChunks !== 0) {
                    $statisticsTable->addRow(new TableSeparator());
                }
                $statisticsTable->addRow([new TableCell('Row Group: ' . $rowGroupIndex, ['colspan' => 7])]);
                $statisticsTable->addRow(new TableSeparator());

                foreach ($rowGroup->columnChunks() as $columnChunk) {
                    $totalChunks++;
                    $statistics = $columnChunk->statistics();
                    /** @var FlatColumn $column */
                    $column = $metadata->schema()->get($columnChunk->flatPath());

                    if ($statistics) {
                        if (ColumnPrimitiveType::isString($column)) {
                            $min = $statistics->min($column) ? StringHumanizer::truncate($statistics->min($column), 20, '...') : '-';
                            $max = $statistics->max($column) ? StringHumanizer::truncate($statistics->max($column), 20, '...') : '-';
                            $minValue = $statistics->minValue($column) ? StringHumanizer::truncate($statistics->minValue($column), 20, '...') : '-';
                            $maxValue = $statistics->maxValue($column) ? StringHumanizer::truncate($statistics->maxValue($column), 20, '...') : '-';
                        } else {
                            $min = $statistics->min($column) ?? '-';
                            $max = $statistics->max($column) ?? '-';
                            $minValue = $statistics->minValue($column) ?? '-';
                            $maxValue = $statistics->maxValue($column) ?? '-';
                        }

                        $nullCount = $statistics->nullCount() ? \number_format($statistics->nullCount()) : '-';
                        $distinctCount = $statistics->distinctCount() ? \number_format($statistics->distinctCount()) : '-';

                        $statisticsTable->addRow([$columnChunk->flatPath(), $min, $max, $minValue, $maxValue, $nullCount, $distinctCount]);
                    } else {
                        $statisticsTable->addRow([
                            $columnChunk->flatPath(),
                            '-',
                            '-',
                            '-',
                            '-',
                            '-',
                            '-',
                        ]);
                    }
                }
            }
            $statisticsTable->setFooterTitle('Total: ' . \number_format($totalChunks));
            $statisticsTable->render();
            $style->newLine();
        }

        if ($displayPageHeaders) {
            $pageHeadersTable = $style->createTable();
            $pageHeadersTable->setStyle('box');
            $pageHeadersTable->setHeaderTitle('Page Headers');
            $pageHeadersTable->setHeaders(['path', 'type', 'encoding', 'compressed size', 'uncompressed size', 'dictionary num values', 'data num values']);
            $totalPageHeaders = 0;

            foreach ($parquetFile->pageHeaders() as $columnPageHeader) {
                $totalPageHeaders++;
                $pageHeadersTable->addRow([
                    $columnPageHeader->column->flatPath(),
                    $columnPageHeader->pageHeader->type()->name,
                    $columnPageHeader->pageHeader->encoding()->name,
                    \number_format($columnPageHeader->pageHeader->compressedPageSize()),
                    \number_format($columnPageHeader->pageHeader->uncompressedPageSize()),
                    $columnPageHeader->pageHeader->dictionaryValuesCount() ? \number_format($columnPageHeader->pageHeader->dictionaryValuesCount()) : '-',
                    $columnPageHeader->pageHeader->dataValuesCount() ? \number_format($columnPageHeader->pageHeader->dataValuesCount()) : '-',
                ]);
            }

            $pageHeadersTable->setFooterTitle('Total: ' . \number_format($totalPageHeaders));
            $pageHeadersTable->render();
            $style->newLine();
        }

        return Command::SUCCESS;
    }
}
