<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Parquet\Engine\Native\NativeParquetFile;
use Generator;

final class NativeParquetOpenSource implements ParquetOpenSource
{
    private ?NativeParquetReader $reader = null;

    public function __construct(
        private readonly NativeParquetFile $file,
    ) {}

    public function batches(Schema $schema, int $batchSize, ?int $offset, ?int $limit, Backend $backend): Generator
    {
        $this->reader?->close();
        $reader = $this->reader = new NativeParquetReader($this->file, $schema, $batchSize, $offset, $limit);

        while (($batch = $reader->next()) !== null) {
            $columns = [];
            $native = $batch->columns();

            // every batch goes through the configured backend, which keeps its own columns and copies the native ones
            foreach ($schema->definitions() as $name => $definition) {
                $columns[$name] = $backend->adopt($definition, $native[$name]);
            }

            yield Rows::fromColumns($schema, $columns, $batch->count());
        }
    }

    public function close(): void
    {
        $this->reader?->close();
    }
}
