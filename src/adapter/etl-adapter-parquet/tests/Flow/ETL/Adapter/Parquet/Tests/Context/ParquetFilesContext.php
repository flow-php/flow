<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\Reader;

use function array_map;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ParquetFilesContext
{
    /**
     * @return array<string>
     */
    public static function columnNames(Filesystem $filesystem, string $uri): array
    {
        return array_map(
            static fn(Column $column): string => $column->name(),
            (new Reader())
                ->readStream($filesystem->readFrom(path($uri)))
                ->schema()
                ->columns(),
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public static function values(Filesystem $filesystem, string $uri): array
    {
        return iterator_to_array(
            (new Reader())
                ->readStream($filesystem->readFrom(path($uri)))
                ->values(),
            false,
        );
    }

    /**
     * @param array<string, Rows> $files - uri => the rows written to it
     */
    public static function write(Filesystem $filesystem, array $files): void
    {
        foreach ($files as $uri => $rows) {
            data_frame()
                ->read(from_rows($rows))
                ->write(to_parquet(path($uri), filesystem: $filesystem))
                ->run();
        }
    }
}
