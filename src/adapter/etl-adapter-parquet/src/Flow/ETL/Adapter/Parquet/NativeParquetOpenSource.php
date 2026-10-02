<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Arrow\Parquet\BatchReader;
use Flow\Arrow\Parquet\ParquetFile;
use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_keys;
use function array_map;

final class NativeParquetOpenSource implements ParquetOpenSource
{
    private ?NativeParquetReader $reader = null;

    public function __construct(
        private readonly ParquetFile $file,
    ) {}

    public function batches(Schema $schema, int $batchSize, ?int $offset, ?int $limit, Backend $backend): Generator
    {
        $this->reader?->close();
        $reader =
            $this->reader = new NativeParquetReader(
                new BatchReader(
                    $this->file,
                    array_map('strval', array_keys($schema->definitions())),
                    $batchSize,
                    $offset,
                    $limit,
                ),
                $schema,
            );

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
