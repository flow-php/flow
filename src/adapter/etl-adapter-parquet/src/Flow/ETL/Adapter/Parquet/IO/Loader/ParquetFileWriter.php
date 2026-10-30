<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Parquet\ParquetFileWriter as ParquetLibFileWriter;

final readonly class ParquetFileWriter implements FileWriter
{
    public function __construct(
        private ParquetLibFileWriter $file,
        private ParquetEncoder $encoder,
        private ParquetSchemaConformance $conformance,
        private Backend $backend,
    ) {}

    public function close(): void
    {
        $this->file->close();
    }

    public function write(Rows $rows): void
    {
        $this->file->writeColumns($this->encoder->columns($this->conformance->conform($rows, $this->backend)));
    }
}
