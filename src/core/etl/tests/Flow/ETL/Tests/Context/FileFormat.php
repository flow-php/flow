<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

interface FileFormat
{
    public function extension(): string;

    public function extractor(
        Path $source,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ): BatchableExtractor&FileExtractor&MetadataColumnsExtractor&SplittableExtractor;

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): Closure&FileLoader;
}
