<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Generator;

use function array_key_exists;
use function sprintf;

/**
 * @import-type DecodedChunk from ColumnReader
 */
final readonly class SelectedPages
{
    public function __construct(
        private ColumnChunkReader $chunks,
    ) {}

    /**
     * The leaf's pages trimmed to the selected rows, so every leaf of every column yields the same rows.
     *
     * @param DecodedChunk $decoded
     *
     * @return Generator<int, ReadFlatColumnValues>
     */
    public function trimmed(
        FlatColumn $leaf,
        RowGroup $rowGroup,
        int $group,
        SourceStream $stream,
        RowSelection $selection,
        Indexes $indexes,
        array $decoded,
    ): Generator {
        foreach ($this->read($leaf, $rowGroup, $group, $stream, $selection, $indexes, $decoded) as [
            $page,
            $pageSelection,
        ]) {
            yield $pageSelection->rowCount() === $pageSelection->totalRows() ? $page : $page->select($pageSelection);
        }
    }

    /**
     * The leaf's whole pages holding a selected row, keyed by the row of the group each starts at, each with the rows
     * of it $selection selects. A page in $decoded is served from it; the others are read, through the offset index
     * when the selection skips rows or decoded pages spare reads. Returns the pages' dictionary.
     *
     * @param DecodedChunk $decoded
     *
     * @return Generator<int, array{ReadFlatColumnValues, RowSelection}, mixed, ?Dictionary>
     */
    public function read(
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

        if ($offsetIndex === null) {
            $pages = $decoded['pages'] === []
                ? $this->chunks->read($rowGroup->getColumnChunk($leaf), $leaf, $stream)
                : (
                    /** @return Generator<int, ReadFlatColumnValues, mixed, ?Dictionary> */
                    static function () use ($decoded): Generator {
                        yield from $decoded['pages'];

                        return $decoded['dictionary'];
                    }
                )();

            foreach ($pages as $firstRow => $page) {
                $pageRows = $page->rowsCount();

                if (($firstRow + $pageRows) > $rows) {
                    throw new RuntimeException(sprintf(
                        'Row group %d decodes more than the %d rows its metadata declares',
                        $group,
                        $rows,
                    ));
                }

                $pageSelection = $selection->slice($firstRow, $pageRows);

                if ($pageSelection->selectsAny()) {
                    yield $firstRow => [$page, $pageSelection];
                }
            }

            return $pages->getReturn();
        }

        $selected = [];
        $fetched = [];

        foreach ($offsetIndex->pages as $page => $location) {
            $pageSelection = $selection->slice($location->firstRowIndex, $offsetIndex->pageRows($page, $rows));

            if ($pageSelection->selectsAny()) {
                $selected[$location->firstRowIndex] = $pageSelection;

                if (!array_key_exists($location->firstRowIndex, $decoded['pages'])) {
                    $fetched[] = $page;
                }
            }
        }

        $read = $this->chunks->read(
            $rowGroup->getColumnChunk($leaf),
            $leaf,
            $stream,
            $offsetIndex,
            $fetched,
            $decoded['dictionary'],
        );

        foreach ($selected as $firstRow => $pageSelection) {
            if (array_key_exists($firstRow, $decoded['pages'])) {
                yield $firstRow => [$decoded['pages'][$firstRow], $pageSelection];

                continue;
            }

            /** @var ReadFlatColumnValues $page one per fetched page, or the read throws */
            $page = $read->current();

            yield $firstRow => [$page, $pageSelection];
            $read->next();
        }

        return $fetched === [] ? $decoded['dictionary'] : $read->getReturn();
    }
}
