<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\ParquetSourceFile;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Reader;

use function Flow\Filesystem\DSL\path;

final class ParquetSourceFileContext
{
    public static function fixture(): Path
    {
        return path(__DIR__ . '/../Integration/Fixtures/Pagination/partitioned/date=2024-01-01/01_1000.parquet');
    }

    /**
     * @param list<string> $columns
     */
    public static function over(Filesystem $filesystem, array $columns = []): ParquetSourceFile
    {
        $stream = $filesystem->readFrom(self::fixture());

        return new ParquetSourceFile(
            (new Reader())->readStream($stream),
            $stream,
            new SourceFile(self::fixture()),
            new SchemaConverter(),
            $columns,
        );
    }
}
