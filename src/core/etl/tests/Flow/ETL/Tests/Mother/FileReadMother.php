<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FileReadMother
{
    /**
     * @param int<1, max> $batchSize
     */
    public static function of(
        Schema $schema,
        ?FileColumns $fileColumns = null,
        int $batchSize = 1000,
        ?Filesystem $filesystem = null,
        ?Path $path = null,
    ): FileRead {
        return new FileRead(
            new SourceFiles($filesystem ?? memory_filesystem(), $path ?? path('memory://')),
            $fileColumns ?? FileColumnsContext::discovering(names: []),
            $schema,
            $batchSize,
        );
    }
}
