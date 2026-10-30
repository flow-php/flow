<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader\ReadPlan;

use function array_keys;

/**
 * The read the adapter does: ReadPlan::forFilter(), its bound conjuncts filtered row by row, read through the reader.
 */
final class FilePlan
{
    /**
     * @param ParquetFile<ParquetFileReader> $file
     */
    public static function of(ParquetFile $file, ?Filter $filter = null, int $offset = 0): ReadPlan
    {
        $footer = $file->reader()->footer();

        return ReadPlan::forFilter(
            $footer,
            $file->indexes(),
            $filter === null ? null : BoundFilter::bind($filter, $footer->schema(), new Options()),
            $offset,
        );
    }

    /**
     * @param ParquetFile<ParquetFileReader> $file
     * @param list<string> $columns
     *
     * @return list<array<string, mixed>>
     */
    public static function rows(ParquetFile $file, array $columns, ReadPlan $plan): array
    {
        $rows = [];

        foreach ($file->reader()->readColumns($columns, 1024, $plan) as $chunk) {
            foreach (array_keys($chunk[$columns[0]]) as $index) {
                $row = [];

                foreach ($columns as $column) {
                    $row[$column] = $chunk[$column][$index];
                }

                $rows[] = $row;
            }
        }

        return $rows;
    }
}
