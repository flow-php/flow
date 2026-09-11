<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\SelfDescribingFile;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Generator;

/**
 * PathFiltering::derivedSchema() is private by design, so a composing class is the only way to reach it.
 */
final class FileReadingExtractor
{
    use FileReading;

    /**
     * @param Generator<int, SelfDescribingFile> $files
     */
    public function derive(Generator $files, bool $unionByName = false): Schema
    {
        return $this->derivedSchema($files, $unionByName);
    }

    /**
     * @return Generator<int, SourceFile>
     */
    public function listing(Filesystem $filesystem, Path $path): Generator
    {
        return $this->sourceFiles($filesystem, $path);
    }
}
