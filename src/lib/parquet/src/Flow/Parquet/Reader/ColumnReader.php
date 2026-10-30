<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Dremel\ColumnData\PagedFlatColumnValues;
use Flow\Parquet\Dremel\ColumnData\ReadFlatColumnValues;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Dremel\ReadColumnData;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\Page\Dictionary;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Generator;

/**
 * One column's values in a row group, the selected rows only.
 *
 * @type DecodedChunk = array{pages: array<int, ReadFlatColumnValues>, dictionary: ?Dictionary}
 */
final readonly class ColumnReader
{
    public function __construct(
        private SelectedPages $pages,
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
            foreach ($this->pages->trimmed(
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
            $flatData[] = new PagedFlatColumnValues($child, $this->pages->trimmed(
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

        $maxDefinitionLevel = $leaf->repetitions()->maxDefinitionLevel();
        $pages = $this->pages->read($leaf, $rowGroup, $group, $stream, $selection, $indexes, $chunk);

        foreach ($pages as $firstRow => [$page, $pageSelection]) {
            $pageValues = $page->rawValues();
            $definitionLevels = $page->definitionLevels();
            $chunk['pages'][$firstRow] = $page;
            $row = 0;
            $valueIndex = 0;

            foreach ($pageSelection->selectors() as $selector) {
                for ($end = $row + $selector->rowCount; $row < $end; $row++) {
                    $defined = $definitionLevels[$row] === $maxDefinitionLevel;

                    if (!$selector->skip) {
                        $values[] = $defined ? $pageValues[$valueIndex] : null;
                    }

                    $valueIndex += $defined ? 1 : 0;
                }
            }
        }

        $chunk['dictionary'] = $pages->getReturn();

        return ['chunk' => $chunk, 'values' => $values];
    }
}
