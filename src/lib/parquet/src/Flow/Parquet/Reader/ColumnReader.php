<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\PagedFlatColumnValues;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;

use function array_values;
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
    public function read(
        Column $column,
        Metadata $metadata,
        SourceStream $stream,
        array $rowGroups,
        Indexes $indexes,
    ): Generator {
        $fileRowGroups = array_values($metadata->rowGroups()->all());

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

            if ($column instanceof FlatColumn) {
                foreach ($this->pages($column, $rowGroup, $group, $stream, $rowGroups[$group], $indexes) as $page) {
                    yield from $this->assembler->assemble($column, new ReadColumnData($column, [
                        $page->flatPath() => $page,
                    ]));
                }

                continue;
            }

            if (!$column instanceof NestedColumn) {
                throw new InvalidArgumentException('Column must be instance of FlatColumn or NestedColumn');
            }

            $flatData = [];

            foreach ($column->childrenFlat() as $child) {
                $flatData[] = new PagedFlatColumnValues($child, $this->pages(
                    $child,
                    $rowGroup,
                    $group,
                    $stream,
                    $rowGroups[$group],
                    $indexes,
                ));
            }

            yield from $this->assembler->assemble($column, new ReadColumnData($column, $flatData));
        }
    }

    /**
     * The leaf's pages trimmed to the selected rows, so every leaf of every column yields the same rows. The offset
     * index is read only when the selection skips rows of the group, and used only when it splits the group's rows.
     *
     * @return Generator<int, ReadFlatColumnValues>
     */
    private function pages(
        FlatColumn $leaf,
        RowGroup $rowGroup,
        int $group,
        SourceStream $stream,
        RowSelection $selection,
        Indexes $indexes,
    ): Generator {
        $rows = $selection->totalRows();
        $whole = $selection->rowCount() === $rows;
        $offsetIndex = $whole ? null : $indexes->offsetIndex($group, $leaf);

        foreach ($this->chunks->read(
            $rowGroup->getColumnChunk($leaf),
            $leaf,
            $stream,
            $offsetIndex?->coversRows($rows) === true ? $offsetIndex : null,
            $selection,
        ) as $firstRow => $page) {
            $pageRows = $page->rowsCount();

            if (($firstRow + $pageRows) > $rows) {
                throw new RuntimeException(sprintf(
                    'Row group %d decodes more than the %d rows its metadata declares',
                    $group,
                    $rows,
                ));
            }

            if ($whole) {
                yield $page;

                continue;
            }

            $pageSelection = $selection->split([$firstRow, $pageRows, $rows - $firstRow - $pageRows])[1];

            if ($pageSelection->selectsAny()) {
                yield $page->select($pageSelection);
            }
        }
    }
}
