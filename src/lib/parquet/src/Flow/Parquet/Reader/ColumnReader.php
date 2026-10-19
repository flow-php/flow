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
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;

use function iterator_to_array;
use function sprintf;

/**
 * One column's values in a row group, the selected rows only.
 *
 * @import-type DecodedPage from ColumnChunkReader
 *
 * @type DecodedChunk = array{pages: array<int, DecodedPage>, dictionary: ?Dictionary}
 */
final readonly class ColumnReader
{
    public function __construct(
        private ColumnChunkReader $chunks,
        private DremelAssembler $assembler,
    ) {}

    /**
     * The column's values at the rows $selection selects, row 0 being the group's first row.
     *
     * @param array<string, DecodedChunk> $decoded chunks readPhysical() returned, by leaf flat path
     */
    public function readRowGroup(
        Column $column,
        RowGroup $rowGroup,
        int $group,
        SourceStream $stream,
        RowSelection $selection,
        Indexes $indexes,
        array $decoded = [],
    ): Generator {
        if (!$selection->selectsAny()) {
            return;
        }

        if ($column instanceof FlatColumn) {
            foreach ($this->pages(
                $column,
                $rowGroup,
                $group,
                $stream,
                $selection,
                $indexes,
                $decoded[$column->flatPath()] ?? ['pages' => [], 'dictionary' => null],
            ) as $page) {
                yield from $this->assembler->assemble($column, new ReadColumnData($column, [
                    $page->flatPath() => $page,
                ]));
            }

            return;
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
                $selection,
                $indexes,
                $decoded[$child->flatPath()] ?? ['pages' => [], 'dictionary' => null],
            ));
        }

        yield from $this->assembler->assemble($column, new ReadColumnData($column, $flatData));
    }

    /**
     * The leaf's undecoded physical values at the rows $selection selects, null where the leaf or an ancestor is null,
     * and the whole decoded pages it read, keyed by the row of the group each starts at, with their dictionary, for
     * readRowGroup() to serve without reading them again.
     *
     * @return array{chunk: DecodedChunk, values: list<mixed>}
     */
    public function readPhysical(
        FlatColumn $leaf,
        RowGroup $rowGroup,
        int $group,
        SourceStream $stream,
        RowSelection $selection,
        Indexes $indexes,
    ): array {
        if ($leaf->maxRepetitionsLevel() > 0) {
            throw new InvalidArgumentException(
                'Column "' . $leaf->flatPath() . '" lies inside a list or a map, where a row holds no single value',
            );
        }

        $chunk = ['pages' => [], 'dictionary' => null];
        $values = [];

        if (!$selection->selectsAny()) {
            return ['chunk' => $chunk, 'values' => $values];
        }

        $read = $this->chunkPages($leaf, $rowGroup, $group, $stream, $selection, $indexes, $chunk);

        foreach ($read as $firstRow => $page) {
            $chunk['pages'][$firstRow] = [
                'values' => iterator_to_array($page->rawValues(), false),
                'repetitionLevels' => $page->repetitionLevels(),
                'definitionLevels' => $page->definitionLevels(),
            ];
        }

        $chunk['dictionary'] = $read->getReturn();

        foreach ($this->pages($leaf, $rowGroup, $group, $stream, $selection, $indexes, $chunk) as $page) {
            foreach ($page->iterator() as $value) {
                $values[] = $value->value;
            }
        }

        return ['chunk' => $chunk, 'values' => $values];
    }

    /**
     * The leaf's whole pages, keyed by the row of the group each starts at: with the offset index those holding a
     * selected row, without it every page. The offset index is read only when the selection skips rows of the group or
     * decoded pages can spare reads, and used only when it splits the group's rows. Returns the pages' dictionary.
     *
     * @param DecodedChunk $decoded
     *
     * @return Generator<int, ReadFlatColumnValues, mixed, ?Dictionary>
     */
    private function chunkPages(
        FlatColumn $leaf,
        RowGroup $rowGroup,
        int $group,
        SourceStream $stream,
        RowSelection $selection,
        Indexes $indexes,
        array $decoded,
    ): Generator {
        $rows = $selection->totalRows();
        $whole = $selection->rowCount() === $rows;
        $offsetIndex = $whole && $decoded['pages'] === [] ? null : $indexes->offsetIndex($group, $leaf);
        $pages = $this->chunks->read(
            $rowGroup->getColumnChunk($leaf),
            $leaf,
            $stream,
            $offsetIndex?->coversRows($rows) === true ? $offsetIndex : null,
            $selection,
            $decoded['pages'],
            $decoded['dictionary'],
        );

        foreach ($pages as $firstRow => $page) {
            $pageRows = $page->rowsCount();

            if (($firstRow + $pageRows) > $rows) {
                throw new RuntimeException(sprintf(
                    'Row group %d decodes more than the %d rows its metadata declares',
                    $group,
                    $rows,
                ));
            }

            yield $firstRow => $page;
        }

        return $pages->getReturn();
    }

    /**
     * The leaf's pages trimmed to the selected rows, so every leaf of every column yields the same rows.
     *
     * @param DecodedChunk $decoded
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
        array $decoded,
    ): Generator {
        $rows = $selection->totalRows();
        $whole = $selection->rowCount() === $rows;

        foreach ($this->chunkPages(
            $leaf,
            $rowGroup,
            $group,
            $stream,
            $selection,
            $indexes,
            $decoded,
        ) as $firstRow => $page) {
            if ($whole) {
                yield $page;

                continue;
            }

            $pageRows = $page->rowsCount();
            $pageSelection = $selection->split([$firstRow, $pageRows, $rows - $firstRow - $pageRows])[1];

            if ($pageSelection->selectsAny()) {
                yield $page->select($pageSelection);
            }
        }
    }
}
