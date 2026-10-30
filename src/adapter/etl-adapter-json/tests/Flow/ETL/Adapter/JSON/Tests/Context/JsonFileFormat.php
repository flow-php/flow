<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Context;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonExtractor;
use Flow\ETL\Adapter\JSON\IO\Loader\JsonLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\to_json;

final readonly class JsonFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'json';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): JsonExtractor
    {
        return from_json($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): JsonLoader
    {
        return to_json($destination, filesystem: $filesystem);
    }
}
