<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function interface_exists;

if (interface_exists(Footer::class, false)) {
    return;
}

interface Footer
{
    /**
     * The whole footer as Metadata, built on the first call.
     */
    public function metadata(): Metadata;

    /**
     * The footer narrowed to the given leaves, without building the row-group metadata of other columns.
     *
     * @param list<FlatColumn> $columns
     */
    public function rowGroupChunks(array $columns): RowGroupChunks;

    public function rowsNumber(): int;

    /**
     * The file schema, without building the row-group metadata.
     */
    public function schema(): Schema;

    /**
     * Σ row group total_byte_size (uncompressed), without building the row-group metadata.
     */
    public function totalByteSize(): int;
}
