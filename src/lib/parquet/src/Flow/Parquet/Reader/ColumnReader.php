<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\PagedFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;

use function count;
use function sprintf;

/**
 * One column's values across the row groups of a file, the selected rows only.
 */
final readonly class ColumnReader
{
    public function __construct(
        private ColumnChunkReader $chunks,
        private DremelAssembler $assembler,
    ) {}

    /**
     * @param list<RowSelection> $rowGroups the selection of each row group, row 0 being the group's first row
     */
    public function read(Column $column, Metadata $metadata, SourceStream $stream, array $rowGroups): Generator
    {
        $fileRowGroups = $metadata->rowGroups()->all();

        if (count($rowGroups) !== count($fileRowGroups)) {
            throw new InvalidArgumentException(sprintf(
                'A selection of %d row groups cannot read a file of %d row groups',
                count($rowGroups),
                count($fileRowGroups),
            ));
        }

        foreach ($fileRowGroups as $group => $rowGroup) {
            if (!$rowGroups[$group]->selectsAny()) {
                continue;
            }

            $selectors = $rowGroups[$group]->selectors();
            $index = 0;
            $remaining = $selectors[0]->rowCount;

            // @mago-ignore analysis:mixed-assignment
            foreach ($this->rows($column, $rowGroup, $stream) as $row) {
                if ($remaining === 0) {
                    $index++;
                    $remaining = (
                        $selectors[$index] ?? throw new RuntimeException(sprintf(
                            'Row group %d decodes more than the %d rows its metadata declares',
                            $group,
                            $rowGroups[$group]->totalRows(),
                        ))
                    )->rowCount;
                }

                if ($selectors[$index]->skip && $index === (count($selectors) - 1)) {
                    break;
                }

                $remaining--;

                if (!$selectors[$index]->skip) {
                    yield $row;
                }
            }
        }
    }

    private function rows(Column $column, RowGroup $rowGroup, SourceStream $stream): Generator
    {
        if ($column instanceof FlatColumn) {
            foreach ($this->chunks->read($rowGroup->getColumnChunk($column), $column, $stream) as $flatColumnValues) {
                yield from $this->assembler->assemble($column, new ReadColumnData($column, [
                    $flatColumnValues->flatPath() => $flatColumnValues,
                ]));
            }

            return;
        }

        if (!$column instanceof NestedColumn) {
            throw new InvalidArgumentException('Column must be instance of FlatColumn or NestedColumn');
        }

        $flatData = [];

        foreach ($column->childrenFlat() as $child) {
            $flatData[] = new PagedFlatColumnValues($child, $this->chunks->read(
                $rowGroup->getColumnChunk($child),
                $child,
                $stream,
            ));
        }

        yield from $this->assembler->assemble($column, new ReadColumnData($column, $flatData));
    }
}
