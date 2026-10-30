<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Context;

use Flow\ETL\Adapter\XML\IO\Extractor\XMLParserExtractor;
use Flow\ETL\Adapter\XML\IO\Loader\XMLLoader;
use Flow\ETL\Tests\Context\FileFormat;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\Adapter\XML\to_xml;

final readonly class XMLFileFormat implements FileFormat
{
    public function extension(): string
    {
        return 'xml';
    }

    public function extractor(Path $source, Filesystem $filesystem = new NativeLocalFilesystem()): XMLParserExtractor
    {
        return from_xml($source, 'rows/row', filesystem: $filesystem);
    }

    public function loader(Path $destination, Filesystem $filesystem = new NativeLocalFilesystem()): XMLLoader
    {
        return to_xml($destination, filesystem: $filesystem);
    }
}
