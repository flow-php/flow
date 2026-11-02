<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor;

use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Physical\Physical;
use Flow\ETL\Values\Column\Physical\PhysicalFor;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\SkippedMatches;
use Flow\Types\Type\Comparator;
use Generator;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_bare;

final class PhpParquetOpenSource implements ParquetOpenSource
{
    public function __construct(
        private readonly ParquetFile $file,
    ) {}

    /**
     * @return Generator<int, Rows|SkippedRows>
     */
    public function batches(Schema $schema, int $batchSize, ReadPlan $plan, Backend $backend): Generator
    {
        $names = array_values(array_map(
            static fn(Definition $definition): string => $definition->name(),
            $schema->definitions(),
        ));

        // a read of no column still counts its rows: the row filter's columns carry them, or the plan alone does
        $read =
            $names !== [] || $plan->rowFilter === null
                ? $names
                : array_values(array_unique(array_map(
                    static fn(FlatColumn $leaf): string => $leaf->path()[0],
                    $plan->rowFilter->columns(),
                )));

        if ($read === []) {
            for (
                $left = min($plan->limit ?? PHP_INT_MAX, $plan->selection->rowCount());
                $left > 0;
                $left -= $batchSize
            ) {
                yield Rows::fromColumns($schema, [], min($batchSize, $left));
            }

            return;
        }

        $converter = new SchemaConverter();
        $comparator = new Comparator();
        /** @var array<string, null|Physical> $logical by column: the physical a declared type other than the file's casts from */
        $logical = [];

        foreach ($schema->definitions() as $definition) {
            $fileType = $this->file->schema()->has($definition->name())
                ? $converter->parquetToFlowType($this->file->schema()->get($definition->name()))
                : null;
            $logical[$definition->name()] = $fileType === null
            || $comparator->equals(type_bare($fileType), type_bare($definition->type()))
                ? null
                : (new PhysicalFor())->physical($fileType);
        }

        $rows =
            /** @param array<string, list<mixed>> $columns */
            static function (array $columns, int $count) use ($schema, $backend, $names, $logical): Rows {
                if ($names === []) {
                    return Rows::fromColumns($schema, [], $count);
                }

                $builder = new RowsBuilder($schema, $backend);

                foreach ($names as $name) {
                    $physical = $logical[$name];

                    // stored values are the physicals of the file's own type, a declared type casts from its values
                    $physical === null
                        ? $builder->column($name)->appendPhysicals($columns[$name])
                        : $builder->column($name)->appendMany($physical->fromPhysicalAll($columns[$name]));
                }

                return $builder->finish();
            };
        $carried = array_fill_keys($read, []);
        $carriedCount = 0;

        foreach ($this->file->reader()->readStoredColumns($read, $batchSize, $plan) as $chunk) {
            if ($chunk instanceof SkippedMatches) {
                yield new SkippedRows($chunk->count);

                continue;
            }

            $chunkCount = count($chunk[$read[0]]);

            if ($carriedCount === 0 && $chunkCount === $batchSize) {
                yield $rows($chunk, $chunkCount);

                continue;
            }

            // an engine may end a chunk at each row group; batches fill across groups as every other source does
            foreach ($read as $name) {
                $carried[$name] = $carriedCount === 0 ? $chunk[$name] : [...$carried[$name], ...$chunk[$name]];
            }

            $carriedCount += $chunkCount;

            while ($carriedCount >= $batchSize) {
                yield $rows(array_map(static fn(array $values): array => array_slice(
                    $values,
                    0,
                    $batchSize,
                ), $carried), $batchSize);
                $carried = array_map(static fn(array $values): array => array_slice($values, $batchSize), $carried);
                $carriedCount -= $batchSize;
            }
        }

        if ($carriedCount > 0) {
            yield $rows($carried, $carriedCount);
        }
    }

    public function close(): void {}
}
