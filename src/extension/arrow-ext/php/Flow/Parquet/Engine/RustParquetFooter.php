<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftModel\FileMetaData;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

/**
 * Created by RustParquetFileReader::footer() only.
 */
final class RustParquetFooter implements Footer
{
    public function metadata(): Metadata
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * A RustRowGroupChunks: the requested leaves' column chunks of every row group, sliced from the footer and
     * decoded on first access.
     *
     * @param list<FlatColumn> $columns
     */
    public function rowGroupChunks(array $columns): RowGroupChunks
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function rowsNumber(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function schema(): Schema
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * The whole footer, decoded on the first call.
     */
    public function thrift(): FileMetaData
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * Σ row group total_byte_size (uncompressed).
     */
    public function totalByteSize(): int
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}
