<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\PagedFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;

/**
 * One column's values across the row groups of a file, from $offset for at most $limit rows.
 */
final readonly class ColumnReader
{
    public function __construct(
        private ColumnChunkReader $chunks,
        private DremelAssembler $assembler,
    ) {}

    public function read(Column $column, Metadata $metadata, SourceStream $stream, ?int $limit, ?int $offset): Generator
    {
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

                foreach ($this->chunks->read(
                    $rowGroup->getColumnChunk($column),
                    $column,
                    $stream,
                ) as $flatColumnValues) {
                    $columnData = new ReadColumnData($column, [$flatColumnValues->flatPath() => $flatColumnValues]);

                    // @mago-ignore analysis:mixed-assignment
                    foreach ($this->assembler->assemble($column, $columnData) as $row) {
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
                $flatData = [];

                foreach ($column->childrenFlat() as $child) {
                    $flatData[] = new PagedFlatColumnValues($child, $this->chunks->read(
                        $rowGroup->getColumnChunk($child),
                        $child,
                        $stream,
                    ));
                }

                $columnData = new ReadColumnData($column, $flatData);

                $rowsSkipped = 0;

                // @mago-ignore analysis:mixed-assignment
                foreach ($this->assembler->assemble($column, $columnData) as $row) {
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
}
