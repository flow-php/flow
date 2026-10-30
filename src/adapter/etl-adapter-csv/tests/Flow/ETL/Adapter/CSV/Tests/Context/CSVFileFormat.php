<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Context;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVExtractor;
use Flow\ETL\Adapter\CSV\IO\Loader\CSVLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;

final readonly class CSVFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'csv';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): CSVExtractor
    {
        return from_csv($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): CSVLoader
    {
        return to_csv($destination, filesystem: $filesystem);
    }
}
