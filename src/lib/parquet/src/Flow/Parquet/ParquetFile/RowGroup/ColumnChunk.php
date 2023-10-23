<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Thrift\ColumnMetaData;

final class ColumnChunk
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
        private readonly PhysicalType $type,
        private readonly Compressions $codec,
        private readonly int $valuesCount,
        private readonly int $fileOffset,
        private readonly array $path,
        private readonly array $encodings,
        private readonly int $totalCompressedSize,
        private readonly int $totalUncompressedSize,
        private readonly ?int $dictionaryPageOffset,
        private readonly ?int $dataPageOffset,
        private readonly ?int $indexPageOffset,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\ColumnChunk $thrift) : self
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
        );
    }

//    public function __debugInfo() : ?array
//    {
//        return $this->normalize();
//    }

    public function codec() : Compressions
    {
        return $this->codec;
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

    public function normalize() : array
    {
        return [
            'file_offset' => $this->fileOffset(),
            'meta_data' => [
                'flat_path' => $this->flatPath(),
                'type' => $this->type()->name,
                'encodings' => \array_map(static fn (Encodings $encoding) => $encoding->name, $this->encodings()),
                'codec' => $this->codec()->name,
                'num_values' => $this->valuesCount(),
                'total_uncompressed_size' => $this->totalUncompressedSize,
                'total_compressed_size' => $this->totalCompressedSize,
                'data_page_offset' => $this->dataPageOffset,
                'index_page_offset' => $this->indexPageOffset,
                'dictionary_page_offset' => $this->dictionaryPageOffset,
            ],
        ];
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     */
    public function pageOffset() : int
    {
        $offset = \min(
            \array_filter(
                [
                    $this->dictionaryPageOffset,
                    $this->dataPageOffset,
                    $this->indexPageOffset,
                ],
            )
        );

        return $offset;
    }

    public function rootName() : string
    {
        return $this->path[0];
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
