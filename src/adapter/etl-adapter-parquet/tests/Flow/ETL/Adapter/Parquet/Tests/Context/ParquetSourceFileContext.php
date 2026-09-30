<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\EngineParquetOpener;
use Flow\ETL\Adapter\Parquet\ParquetOpener;
use Flow\ETL\Adapter\Parquet\ParquetSourceFile;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;

use function Flow\Filesystem\DSL\path;

final class ParquetSourceFileContext
{
    public static function fixture(): Path
    {
        return path(__DIR__ . '/../Integration/Fixtures/Pagination/partitioned/date=2024-01-01/01_1000.parquet');
    }

    /**
     * @template R of \Flow\Parquet\ParquetFileReader
     *
     * @param list<string> $columns
     * @param ParquetOpener<R> $opener
     *
     * @return ParquetSourceFile<R>
     */
    public static function over(
        Filesystem $filesystem,
        array $columns = [],
        ParquetOpener $opener = new EngineParquetOpener(new PhpParquetEngine(), new Options()),
    ): ParquetSourceFile {
        return new ParquetSourceFile(
            $opener->file($filesystem->readFrom(self::fixture())),
            new SourceFile(self::fixture()),
            new SchemaConverter(),
            $columns,
        );
    }
}
