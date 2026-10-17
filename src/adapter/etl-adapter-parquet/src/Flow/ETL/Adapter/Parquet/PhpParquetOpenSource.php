<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\Reader\ReadPlan;
use Generator;

use function array_map;
use function array_values;

final class PhpParquetOpenSource implements ParquetOpenSource
{
    public function __construct(
        private readonly ParquetFile $file,
    ) {}

    /**
     * @return Generator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, ReadPlan $plan, Backend $backend): Generator
    {
        $names = array_values(array_map(static fn(Definition $definition): string => $definition
            ->entry()
            ->name(), $schema->definitions()));

        foreach ($this->file->reader()->readColumns($names, $batchSize, $plan) as $chunk) {
            $builder = new RowsBuilder($schema, $backend);

            foreach ($names as $name) {
                $builder->column($name)->appendMany($chunk[$name]);
            }

            yield $builder->finish();
        }
    }

    public function close(): void {}
}
