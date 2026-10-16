<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
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
     * @param null|int $dataPageOffset
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
        private ?int $dataPageOffset,
        private ?int $indexPageOffset,
        private ?Statistics $statistics,
    ) {}

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
        );
    }

    public function codec(): Compressions
    {
        return $this->codec;
    }

    public function dataPageOffset(): ?int
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
            ]),
        ]);
    }

    public function type(): PhysicalType
    {
        return $this->type;
    }

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }
}
