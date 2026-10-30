<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Floe\FloeExtractor;
use Flow\ETL\IO\Floe\FloeLoader;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final readonly class FloeFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'floe';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): FloeExtractor
    {
        return from_floe($source, filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): FloeLoader
    {
        return to_floe($destination, filesystem: $filesystem);
    }
}
