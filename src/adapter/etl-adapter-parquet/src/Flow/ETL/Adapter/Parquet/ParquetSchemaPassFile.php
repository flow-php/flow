<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Extractor\File\SelfDescribingFile;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Schema;

/**
 * The schema pass's first file: close() leaves it open for the read that follows; the extractor closes it.
 */
final readonly class ParquetSchemaPassFile implements SelfDescribingFile
{
    public function __construct(
        private ParquetSourceFile $file,
    ) {}

    public function close(): void {}

    public function schema(): Schema
    {
        return $this->file->schema();
    }

    public function source(): SourceFile
    {
        return $this->file->source();
    }

    public function statistics(): Statistics
    {
        return $this->file->statistics();
    }
}
