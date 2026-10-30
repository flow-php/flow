<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Context;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelExtractor;
use Flow\ETL\Adapter\Excel\IO\Loader\ExcelLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\Excel\DSL\from_excel;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;

final readonly class ExcelFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'xlsx';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): ExcelExtractor
    {
        return from_excel($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): ExcelLoader
    {
        return to_excel($destination, filesystem: $filesystem);
    }
}
