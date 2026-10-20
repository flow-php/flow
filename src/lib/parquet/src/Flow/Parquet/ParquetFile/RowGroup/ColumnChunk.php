<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Statistics;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use Flow\Parquet\ThriftModel\ColumnMetaData;

use function array_filter;
use function array_map;
use function implode;
use function min;

final readonly class ColumnChunk
{
    /**
     * @param PhysicalType $type
     * @param Compressions $codec
     * @param int $valuesCount
     * @param int $fileOffset
     * @param array<string> $path
     * @param array<Encodings> $encodings
     * @param int $totalCompressedSize
     * @param int $totalUncompressedSize
     * @param null|int $dictionaryPageOffset
     * @param int $dataPageOffset
     * @param null|int $indexPageOffset
     */
    public function __construct(
        private PhysicalType $type,
        private Compressions $codec,
        private int $valuesCount,
        private int $fileOffset,
        private array $path,
        private array $encodings,
        private int $totalCompressedSize,
        private int $totalUncompressedSize,
        private ?int $dictionaryPageOffset,
        private int $dataPageOffset,
        private ?int $indexPageOffset,
        private ?Statistics $statistics,
        private ?IndexLocation $columnIndex = null,
        private ?IndexLocation $offsetIndex = null,
        private ?int $bloomFilterOffset = null,
        private ?int $bloomFilterLength = null,
    ) {
        if ($bloomFilterLength !== null && $bloomFilterOffset === null) {
            throw new InvalidArgumentException('A column chunk records a bloom filter length without its offset');
        }
    }

    public static function fromThrift(ThriftColumnChunk $thrift): self
    {
        $metadata = $thrift->meta_data ?? throw new InvalidArgumentException(
            'Column chunk at offset '
            . $thrift->file_offset
            . ' has no metadata (an encrypted column is not supported)',
        );

        return new self(
            PhysicalType::from($metadata->type),
            Compressions::from($metadata->codec),
            (int) $metadata->num_values,
            (int) $thrift->file_offset,
            $metadata->path_in_schema,
            array_map(static fn($encoding) => Encodings::from($encoding), $metadata->encodings),
            (int) $metadata->total_compressed_size,
            (int) $metadata->total_uncompressed_size,
            $metadata->dictionary_page_offset !== null ? (int) $metadata->dictionary_page_offset : null,
            (int) $metadata->data_page_offset,
            $metadata->index_page_offset !== null ? (int) $metadata->index_page_offset : null,
            $metadata->statistics !== null ? Statistics::fromThrift($metadata->statistics) : null,
            IndexLocation::fromThrift($thrift->column_index_offset, $thrift->column_index_length),
            IndexLocation::fromThrift($thrift->offset_index_offset, $thrift->offset_index_length),
            $metadata->bloom_filter_offset !== null ? (int) $metadata->bloom_filter_offset : null,
            $metadata->bloom_filter_offset !== null && $metadata->bloom_filter_length !== null
                ? (int) $metadata->bloom_filter_length
                : null,
        );
    }

    /**
     * Null when the writer did not record it: the header is then read before the bitset.
     */
    public function bloomFilterLength(): ?int
    {
        return $this->bloomFilterLength;
    }

    /**
     * Null when the chunk has no bloom filter.
     */
    public function bloomFilterOffset(): ?int
    {
        return $this->bloomFilterOffset;
    }

    public function codec(): Compressions
    {
        return $this->codec;
    }

    public function columnIndex(): ?IndexLocation
    {
        return $this->columnIndex;
    }

    public function dataPageOffset(): int
    {
        return $this->dataPageOffset;
    }

    public function dictionaryPageOffset(): ?int
    {
        return $this->dictionaryPageOffset;
    }

    /**
     * @return array<Encodings>
     */
    public function encodings(): array
    {
        return $this->encodings;
    }

    public function fileOffset(): int
    {
        return $this->fileOffset;
    }

    public function flatPath(): string
    {
        return implode('.', $this->path);
    }

    public function offsetIndex(): ?IndexLocation
    {
        return $this->offsetIndex;
    }

    public function pageOffset(): int
    {
        $offsets = array_filter(
            [
                $this->dictionaryPageOffset,
                $this->dataPageOffset,
                $this->indexPageOffset,
            ],
            static fn(?int $offset): bool => $offset !== null,
        );

        if ($offsets === []) {
            throw new RuntimeException('ColumnChunk has no page offsets');
        }

        return min($offsets);
    }

    public function statistics(): ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics);
    }

    public function totalCompressedSize(): int
    {
        return $this->totalCompressedSize;
    }

    public function totalUncompressedSize(): int
    {
        return $this->totalUncompressedSize;
    }

    public function toThrift(): ThriftColumnChunk
    {
        return new ThriftColumnChunk([
            'file_offset' => $this->fileOffset,
            'meta_data' => new ColumnMetaData([
                'type' => $this->type->value,
                'encodings' => array_map(static fn(Encodings $encoding) => $encoding->value, $this->encodings),
                'path_in_schema' => $this->path,
                'codec' => $this->codec->value,
                'num_values' => $this->valuesCount,
                'total_uncompressed_size' => $this->totalUncompressedSize,
                'total_compressed_size' => $this->totalCompressedSize,
                'data_page_offset' => $this->dataPageOffset,
                'index_page_offset' => $this->indexPageOffset,
                'dictionary_page_offset' => $this->dictionaryPageOffset,
                'statistics' => $this->statistics?->toThrift(),
                'bloom_filter_offset' => $this->bloomFilterOffset,
                'bloom_filter_length' => $this->bloomFilterLength,
            ]),
            'column_index_offset' => $this->columnIndex?->offset,
            'column_index_length' => $this->columnIndex?->length,
            'offset_index_offset' => $this->offsetIndex?->offset,
            'offset_index_length' => $this->offsetIndex?->length,
        ]);
    }

    public function type(): PhysicalType
    {
        return $this->type;
    }

    /**
     * This chunk with its page index and its bloom filter (header and bitset) written at the given locations; a
     * location left out is kept.
     */
    public function withLocations(
        ?IndexLocation $columnIndex = null,
        ?IndexLocation $offsetIndex = null,
        ?int $bloomFilterOffset = null,
        ?int $bloomFilterLength = null,
    ): self {
        return new self(
            $this->type,
            $this->codec,
            $this->valuesCount,
            $this->fileOffset,
            $this->path,
            $this->encodings,
            $this->totalCompressedSize,
            $this->totalUncompressedSize,
            $this->dictionaryPageOffset,
            $this->dataPageOffset,
            $this->indexPageOffset,
            $this->statistics,
            $columnIndex ?? $this->columnIndex,
            $offsetIndex ?? $this->offsetIndex,
            $bloomFilterOffset ?? $this->bloomFilterOffset,
            $bloomFilterLength ?? $this->bloomFilterLength,
        );
    }

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }
}
