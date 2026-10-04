<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFileReader;

use function array_keys;
use function count;

/**
 * A file's rows read through columns() in batches of a given size, flattened back to rows.
 */
final class ParquetRows
{
    /**
     * @param ParquetFile<ParquetFileReader> $file
     * @param int<1, max> $batchSize
     * @param list<string> $columns
     *
     * @return list<array<string, mixed>>
     */
    public static function read(
        ParquetFile $file,
        int $batchSize,
        array $columns = [],
        ?int $limit = null,
        ?int $offset = null,
    ): array {
        $rows = [];

        foreach ($file->columns($batchSize, $columns, $limit, $offset) as $chunk) {
            $names = array_keys($chunk);

            for ($i = 0, $count = count($chunk[$names[0]]); $i < $count; $i++) {
                $row = [];

                foreach ($names as $name) {
                    $row[$name] = $chunk[$name][$i];
                }

                $rows[] = $row;
            }
        }

        return $rows;
    }
}
