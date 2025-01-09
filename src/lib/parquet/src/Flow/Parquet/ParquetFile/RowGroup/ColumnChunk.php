<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\{Compressions, Encodings, Statistics};
use Flow\Parquet\Thrift\ColumnMetaData;

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
        private Options $options,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\ColumnChunk $thrift, Options $options) : self
    {
        return new self(
            PhysicalType::from($thrift->meta_data->type),
            Compressions::from($thrift->meta_data->codec),
            $thrift->meta_data->num_values,
            $thrift->file_offset,
            $thrift->meta_data->path_in_schema,
            \array_map(static fn ($encoding) => Encodings::from($encoding), $thrift->meta_data->encodings),
            $thrift->meta_data->total_compressed_size,
            $thrift->meta_data->total_uncompressed_size,
            $thrift->meta_data->dictionary_page_offset,
            $thrift->meta_data->data_page_offset,
            $thrift->meta_data->index_page_offset,
            $thrift->meta_data->statistics ? Statistics::fromThrift($thrift->meta_data->statistics) : null,
            $options
        );
    }

    public function codec() : Compressions
    {
        return $this->codec;
    }

    public function dataPageOffset() : ?int
    {
        return $this->dataPageOffset;
    }

    public function dictionaryPageOffset() : ?int
    {
        return $this->dictionaryPageOffset;
    }

    /**
     * @return array<Encodings>
     */
    public function encodings() : array
    {
        return $this->encodings;
    }

    public function fileOffset() : int
    {
        return $this->fileOffset;
    }

    public function flatPath() : string
    {
        return \implode('.', $this->path);
    }

    public function pageOffset() : int
    {
        return \min(
            // @phpstan-ignore-next-line
            \array_filter(
                [
                    $this->dictionaryPageOffset,
                    $this->dataPageOffset,
                    $this->indexPageOffset,
                ],
            )
        );
    }

    public function statistics() : ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics, $this->options);
    }

    public function totalCompressedSize() : int
    {
        return $this->totalCompressedSize;
    }

    public function totalUncompressedSize() : int
    {
        return $this->totalUncompressedSize;
    }

    public function toThrift() : \Flow\Parquet\Thrift\ColumnChunk
    {
        return new \Flow\Parquet\Thrift\ColumnChunk([
            'file_offset' => $this->fileOffset,
            'meta_data' => new ColumnMetaData([
                'type' => $this->type->value,
                'encodings' => \array_map(static fn (Encodings $encoding) => $encoding->value, $this->encodings),
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

    public function type() : PhysicalType
    {
        return $this->type;
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
