<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\FileMetaData;
use Generator;
use MultipleIterator;

use function array_map;
use function array_push;
use function count;
use function is_array;
use function iterator_to_array;
use function unpack;

final class PhpParquetEngine implements ParquetEngine
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private readonly Options $options = new Options(),
    ) {}

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $this->options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );
    }

    public function readValues(
        SourceStream $stream,
        Schema $schema,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
    ): Generator {
        $dataConverter = DataConverter::initialize($this->options);
        $dremelAssembler = new DremelAssembler($dataConverter);
        $chunkReader = new ColumnChunkReader(new PageReader($this->byteOrder, $this->options), $this->options);

        $metadata = $this->readMetadata($stream);

        if (!count($columns)) {
            $columns = array_map(static fn(Column $c) => $c->name(), $schema->columns());
        }

        $totalRows = $metadata->rowsNumber();

        if ($offset !== null && $offset > $totalRows) {
            return;
        }

        if ($offset !== null) {
            if ($totalRows > $offset) {
                $totalRows -= $offset;
            } else {
                $totalRows = 0;
            }
        }

        $totalRows = min($totalRows, $limit ?? $totalRows);

        if ($totalRows === 0) {
            return;
        }

        $multipleIterator = new MultipleIterator(MultipleIterator::MIT_KEYS_ASSOC);

        foreach ($columns as $columnName) {
            $multipleIterator->attachIterator(
                $this->readColumn(
                    $schema->get($columnName),
                    $metadata,
                    $stream,
                    $chunkReader,
                    $dremelAssembler,
                    $limit,
                    $offset,
                ),
                $columnName,
            );
        }

        $rowCount = 0;

        foreach ($multipleIterator as $rowData) {
            if ($limit !== null && $rowCount >= $limit) {
                break;
            }

            $row = [];

            // Row payload assembled from per-column generators; values are user data (mixed).
            // @mago-ignore analysis:mixed-assignment
            foreach ($rowData as $columnData) {
                if (is_array($columnData)) {
                    // @mago-ignore analysis:mixed-assignment
                    foreach ($columnData as $key => $value) {
                        $row[(string) $key] = $value;
                    }
                }
            }

            yield $row;
            $rowCount++;
        }
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        // writeRows() reads the row group check interval from its own $options, openForWrite() from the engine's
        $file = new PhpParquetFileWriter(
            $stream,
            $schema,
            $compression,
            $options,
            $options->getInt(Option::ROW_GROUP_SIZE_CHECK_INTERVAL),
        );

        foreach ($rows as $row) {
            $file->writeRow($row);
        }

        $file->close();
    }

    private function readColumn(
        Column $column,
        Metadata $metadata,
        SourceStream $stream,
        ColumnChunkReader $chunkReader,
        DremelAssembler $dremelAssembler,
        ?int $limit,
        ?int $offset,
    ): Generator {
        $yieldedRows = 0;
        $rowGroupOffset = 0;

        foreach ($metadata->rowGroups()->all() as $rowGroup) {
            if ($offset !== null) {
                if (($rowGroupOffset + $rowGroup->rowsCount()) <= $offset) {
                    $rowGroupOffset += $rowGroup->rowsCount();

                    continue;
                }
            }
            $skipRows = $offset !== null ? $offset - $rowGroupOffset : 0;

            if ($column instanceof FlatColumn) {
                $rowsSkipped = 0;

                foreach ($chunkReader->read(
                    $rowGroup->getColumnChunk($column),
                    $column,
                    $stream,
                ) as $flatColumnValues) {
                    $columnData = new ReadColumnData($column, [$flatColumnValues->flatPath() => $flatColumnValues]);

                    // @mago-ignore analysis:mixed-assignment
                    foreach ($dremelAssembler->assemble($column, $columnData) as $row) {
                        if ($skipRows > 0 && $rowsSkipped < $skipRows) {
                            $rowsSkipped++;

                            continue;
                        }

                        if ($limit !== null && $yieldedRows >= $limit) {
                            return;
                        }
                        yield $row;
                        $yieldedRows++;
                    }
                }
            } elseif ($column instanceof NestedColumn) {
                $mergedFlatData = [];

                foreach ($column->childrenFlat() as $child) {
                    $pages = iterator_to_array($chunkReader->read($rowGroup->getColumnChunk($child), $child, $stream));

                    $allRepetitionLevels = [];
                    $allDefinitionLevels = [];

                    foreach ($pages as $page) {
                        array_push($allRepetitionLevels, ...$page->repetitionLevels());
                        array_push($allDefinitionLevels, ...$page->definitionLevels());
                    }

                    $mergedFlatData[] = new ReadFlatColumnValues(
                        $child,
                        (static function () use ($pages) {
                            foreach ($pages as $page) {
                                yield from $page->rawValues();
                            }
                        })(),
                        $allRepetitionLevels,
                        $allDefinitionLevels,
                    );
                }

                $columnData = new ReadColumnData($column, $mergedFlatData);

                $rowsSkipped = 0;

                // @mago-ignore analysis:mixed-assignment
                foreach ($dremelAssembler->assemble($column, $columnData) as $row) {
                    if ($skipRows > 0 && $rowsSkipped < $skipRows) {
                        $rowsSkipped++;

                        continue;
                    }

                    if ($limit !== null && $yieldedRows >= $limit) {
                        return;
                    }
                    yield $row;
                    $yieldedRows++;
                }
            } else {
                throw new InvalidArgumentException('Column must be instance of FlatColumn or NestedColumn');
            }

            $rowGroupOffset += $rowGroup->rowsCount();
        }
    }

    private function readMetadata(SourceStream $stream): Metadata
    {
        $fileTotalSize = $stream->size();

        if ($fileTotalSize === null) {
            throw new InvalidArgumentException('Cannot determine Parquet file size');
        }

        if ($stream->read(4, $fileTotalSize - 4) !== ParquetFile::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        $unpacked = unpack($this->byteOrder->value, $stream->read(4, $fileTotalSize - 8));

        if ($unpacked === false) {
            throw new InvalidArgumentException('Failed to read Parquet metadata length');
        }

        $metadataLength = $unpacked[1];

        if ($metadataLength <= 0) {
            throw new InvalidArgumentException('Parquet metadata length must be positive, got ' . $metadataLength);
        }

        $metadata = $stream->read($metadataLength, $fileTotalSize - ($metadataLength + 8));

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new CompactProtocol(new MemoryBuffer($metadata)));

        return Metadata::fromThrift($thriftMetadata);
    }
}
