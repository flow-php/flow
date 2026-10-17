<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ThriftModel\FileMetaData;

final class PhpParquetFooter implements Footer
{
    private ?Metadata $metadata = null;

    public function __construct(
        private readonly FileMetaData $thrift,
    ) {}

    public function metadata(): Metadata
    {
        return $this->metadata ??= Metadata::fromThrift($this->thrift);
    }

    public function rowGroupChunks(array $columns): RowGroupChunks
    {
        return new MetadataRowGroupChunks($this->metadata());
    }

    public function rowsNumber(): int
    {
        return $this->metadata()->rowsNumber();
    }

    public function schema(): Schema
    {
        return $this->metadata()->schema();
    }

    public function totalByteSize(): int
    {
        $bytes = 0;

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            $bytes += $rowGroup->totalByteSize();
        }

        return $bytes;
    }
}
