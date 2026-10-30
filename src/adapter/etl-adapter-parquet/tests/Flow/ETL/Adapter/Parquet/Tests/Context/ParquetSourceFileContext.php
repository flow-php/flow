<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetSourceFile;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;

use function dirname;
use function Flow\Filesystem\DSL\path;

final class ParquetSourceFileContext
{
    public static function fixture(): Path
    {
        return path(self::pagination() . '/partitioned/date=2024-01-01/01_1000.parquet');
    }

    public static function pagination(): string
    {
        return dirname(__DIR__) . '/Fixtures/Pagination';
    }

    /**
     * @param list<string> $columns
     */
    public static function over(
        Filesystem $filesystem,
        array $columns = [],
        ParquetEngine $engine = new PhpParquetEngine(),
        ?FlowPhpExtension $extension = null,
    ): ParquetSourceFile {
        $stream = $filesystem->readFrom(self::fixture());

        return new ParquetSourceFile(
            new ParquetFile($stream, new Options(), $engine->openForRead($stream)),
            new SourceFile(self::fixture()),
            new SchemaConverter(),
            $columns,
            $extension,
        );
    }
}
