<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    private array $data = [];

    public function __construct(private readonly FlatColumn $column, private readonly DataConverter $dataConverter)
    {
    }

    public function addRow(mixed $data) : void
    {
        $this->data[] = $data;
    }

    public function flush(int $fileOffset) : ColumnChunkContainer
    {
        return $this->createColumnChunkContainer(
            (new PagesBuilder($this->dataConverter))->build($this->column, $this->data),
            $fileOffset
        );
    }

    /**
     * @param array<PageContainer> $pageContainers
     */
    private function createColumnChunkContainer(array $pageContainers, int $offset) : ColumnChunkContainer
    {
        $buffer = '';
        $encodings = [];
        $valuesCount = 0;
        $size = 0;
        $dictionaryPageSize = null;
        $dictionaryPageOffset = null;
        $pageOffset = $offset;

        foreach ($pageContainers as $pageContainer) {
            if ($pageContainer->pageHeader->type() === Type::DICTIONARY_PAGE) {
                if ($dictionaryPageSize !== null) {
                    throw new RuntimeException('There can be only one dictionary page in column chunk');
                }

                $dictionaryPageOffset = $pageOffset;
                $dictionaryPageSize = $pageContainer->size();
            }

            $buffer .= $pageContainer->pageHeaderBuffer . $pageContainer->pageBuffer;
            $encodings[] = $pageContainer->pageHeader->encoding()->value;
            $valuesCount += \count($pageContainer->values);
            $size += $pageContainer->size();
            $pageOffset += $pageContainer->size();
        }

        $encodings = \array_values(\array_unique($encodings));
        $encodings = \array_map(static fn (int $encoding) => Encodings::from($encoding), $encodings);

        return new ColumnChunkContainer(
            $buffer,
            new ColumnChunk(
                type: $this->column->type(),
                codec: Compressions::UNCOMPRESSED,
                valuesCount: $valuesCount,
                fileOffset: $offset,
                path: $this->column->path(),
                encodings: $encodings,
                totalCompressedSize: $size,
                totalUncompressedSize: $size,
                dictionaryPageOffset: $dictionaryPageOffset,
                dataPageOffset: ($dictionaryPageOffset) ? $offset + $dictionaryPageSize : $offset,
                indexPageOffset: null,
            )
        );
    }
}
