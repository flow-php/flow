<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Context;

use Flow\ETL\Adapter\Text\IO\Extractor\TextExtractor;
use Flow\ETL\Adapter\Text\IO\Loader\TextLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;

final readonly class TextFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'txt';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): TextExtractor
    {
        return from_text($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): TextLoader
    {
        return to_text($destination, filesystem: $filesystem);
    }
}
