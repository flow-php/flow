<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\IO\Loader\ParquetLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;

final readonly class ParquetFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'parquet';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): ParquetExtractor
    {
        return from_parquet($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): ParquetLoader
    {
        return to_parquet($destination, filesystem: $filesystem);
    }
}
