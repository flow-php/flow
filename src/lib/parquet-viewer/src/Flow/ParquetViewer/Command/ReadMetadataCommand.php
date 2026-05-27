<?php

declare(strict_types=1);

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

use function array_map;
use function count;
use function file_exists;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function implode;
use function number_format;
use function realpath;
use function sprintf;

#[AsCommand(name: 'read:metadata', description: 'Read metadata from parquet file')]
final class ReadMetadataCommand extends Command
{
    protected function configure(): void
    {
        $this
            ->addArgument('file', InputArgument::REQUIRED, 'path to parquet file')
            ->addOption('columns', null, InputOption::VALUE_NONE, 'Display column details')
            ->addOption('row-groups', null, InputOption::VALUE_NONE, 'Display row group details')
            ->addOption('column-chunks', null, InputOption::VALUE_NONE, 'Display column chunks details')
            ->addOption('statistics', null, InputOption::VALUE_NONE, 'Display column chunks statistics details')
            ->addOption('page-headers', null, InputOption::VALUE_NONE, 'Display page headers details');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $style = new SymfonyStyle($input, $output);
        $filePath = type_string()->assert($input->getArgument('file'));

        if (!file_exists($filePath)) {
            $style->error(sprintf('File "%s" does not exist', $filePath));

            return Command::FAILURE;
        }

        $displayColumns = type_boolean()->cast($input->getOption('columns'));
        $displayRowGroups = type_boolean()->cast($input->getOption('row-groups'));
        $displayStatistics = type_boolean()->cast($input->getOption('statistics'));
        $displayColumnChunks = type_boolean()->cast($input->getOption('column-chunks'));
        $displayPageHeaders = type_boolean()->cast($input->getOption('page-headers'));

        $reader = new Reader();
        $parquetFile = $reader->read($filePath);

        try {
            $metadata = $parquetFile->metadata();
        } catch (InvalidArgumentException $_e) {
            $style->error(sprintf('File "%s" is not a valid parquet file', $filePath));

            return Command::FAILURE;
        }

        $metadataTable = $style->createTable();

        $metadataTable->setHeaderTitle('Metadata');
        $metadataTable->setStyle('box');
        $metadataTable->setHorizontal();
        $metadataTable->setHeaders(['file path', 'parquet version', 'created by', 'rows']);
        $metadataTable->setRows([
            [
                realpath($filePath),
                $metadata->version(),
                $metadata->createdBy(),
                number_format($metadata->rowsNumber(), 0, '.', ','),
            ],
        ]);

        $metadataTable->render();
        $style->newLine();

        if ($displayColumns) {
            $columnsTable = $style->createTable();
            $columnsTable->setStyle('box');
            $columnsTable->setHeaderTitle('Columns');
            $columnsTable->setHeaders([
                'path',
                'type',
                'logical type',
                'repetition',
                'max repetition',
                'max definition',
            ]);

            foreach ($parquetFile->schema()->columnsFlat() as $column) {
                $logicalType = $column->logicalType();
                $typeLength = $column->typeLength();
                $columnsTable->addRow([
                    $column->flatPath(),
                    $column->type()->name . ($typeLength ? '(' . $typeLength . ')' : ''),
                    $logicalType ? $logicalType->name() : '-',
                    $column->repetition()?->name ?: 'N/A',
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
                    number_format($rowGroup->rowsCount()),
                    number_format($rowGroup->totalByteSize()),
                    count($rowGroup->columnChunks()),
                ]);
            }

            $rowGroupsTable->setFooterTitle('Total: ' . number_format($totalRowGroups));
            $rowGroupsTable->render();
            $style->newLine();
        }

        if ($displayColumnChunks) {
            $chunksTable = $style->createTable();
            $chunksTable->setStyle('box');
            $chunksTable->setHeaderTitle('Column Chunks');
            $chunksTable->setHeaders([
                'path',
                'encodings',
                'compression',
                'file offset',
                'num values',
                'rows_count',
                'dictionary page offset',
                'data page offset',
            ]);
            $totalChunks = 0;

            foreach ($metadata->rowGroups()->all() as $rowGroup) {
                foreach ($rowGroup->columnChunks() as $columnChunk) {
                    $totalChunks++;
                    $dictionaryPageOffset = $columnChunk->dictionaryPageOffset();
                    $dataPageOffset = $columnChunk->dataPageOffset();
                    $chunksTable->addRow([
                        $columnChunk->flatPath(),
                        '[' . implode(',', array_map(static fn($e) => $e->name, $columnChunk->encodings())) . ']',
                        $columnChunk->codec()->name,
                        number_format($columnChunk->fileOffset()),
                        number_format($columnChunk->valuesCount()),
                        $dictionaryPageOffset ? number_format($dictionaryPageOffset) : '-',
                        $dataPageOffset ? number_format($dataPageOffset) : '-',
                    ]);
                }
            }
            $chunksTable->setFooterTitle('Total: ' . number_format($totalChunks));
            $chunksTable->render();
            $style->newLine();
        }

        if ($displayStatistics) {
            $statisticsTable = $style->createTable();
            $statisticsTable->setStyle('box');
            $statisticsTable->setHeaderTitle('Column Chunks Statistics');
            $statisticsTable->setHeaders([
                'path',
                'min [deprecated]',
                'max [deprecated]',
                'min value',
                'max value',
                'null count',
                'distinct count',
            ]);
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
                            $minVal = type_optional(type_scalar())->assert($statistics->min($column));
                            $min = $minVal ? StringHumanizer::truncate((string) $minVal, 20, '...') : '-';
                            $maxVal = type_optional(type_scalar())->assert($statistics->max($column));
                            $max = $maxVal ? StringHumanizer::truncate((string) $maxVal, 20, '...') : '-';
                            $minValueVal = type_optional(type_scalar())->assert($statistics->minValue($column));
                            $minValue = $minValueVal
                                ? StringHumanizer::truncate((string) $minValueVal, 20, '...')
                                : '-';
                            $maxValueVal = type_optional(type_scalar())->assert($statistics->maxValue($column));
                            $maxValue = $maxValueVal
                                ? StringHumanizer::truncate((string) $maxValueVal, 20, '...')
                                : '-';
                        } else {
                            $min = type_scalar()->assert($statistics->min($column) ?? '-');
                            $max = type_scalar()->assert($statistics->max($column) ?? '-');
                            $minValue = type_scalar()->assert($statistics->minValue($column) ?? '-');
                            $maxValue = type_scalar()->assert($statistics->maxValue($column) ?? '-');
                        }

                        $nullCountVal = $statistics->nullCount();
                        $nullCount = $nullCountVal ? number_format($nullCountVal) : '-';
                        $distinctCountVal = $statistics->distinctCount();
                        $distinctCount = $distinctCountVal ? number_format($distinctCountVal) : '-';

                        $statisticsTable->addRow([
                            $columnChunk->flatPath(),
                            $min,
                            $max,
                            $minValue,
                            $maxValue,
                            $nullCount,
                            $distinctCount,
                        ]);
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
            $statisticsTable->setFooterTitle('Total: ' . number_format($totalChunks));
            $statisticsTable->render();
            $style->newLine();
        }

        if ($displayPageHeaders) {
            $pageHeadersTable = $style->createTable();
            $pageHeadersTable->setStyle('box');
            $pageHeadersTable->setHeaderTitle('Page Headers');
            $pageHeadersTable->setHeaders([
                'path',
                'type',
                'encoding',
                'compressed size',
                'uncompressed size',
                'dictionary num values',
                'data num values',
            ]);
            $totalPageHeaders = 0;

            foreach ($parquetFile->pageHeaders() as $columnPageHeader) {
                $totalPageHeaders++;
                $dictionaryValuesCount = $columnPageHeader->pageHeader->dictionaryValuesCount();
                $dataValuesCount = $columnPageHeader->pageHeader->dataValuesCount();
                $pageHeadersTable->addRow([
                    $columnPageHeader->column->flatPath(),
                    $columnPageHeader->pageHeader->type()->name,
                    $columnPageHeader->pageHeader->encoding()->name,
                    number_format($columnPageHeader->pageHeader->compressedPageSize()),
                    number_format($columnPageHeader->pageHeader->uncompressedPageSize()),
                    $dictionaryValuesCount ? number_format($dictionaryValuesCount) : '-',
                    $dataValuesCount ? number_format($dataValuesCount) : '-',
                ]);
            }

            $pageHeadersTable->setFooterTitle('Total: ' . number_format($totalPageHeaders));
            $pageHeadersTable->render();
            $style->newLine();
        }

        return Command::SUCCESS;
    }
}
