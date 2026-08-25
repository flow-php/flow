<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesSink;

use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

abstract class FilesSinkTestCase extends FlowIntegrationTestCase
{
    abstract protected function saveMode(): SaveMode;

    protected function filesDirectory(): string
    {
        return __DIR__ . '/tmp';
    }

    protected function files(Path $destination, ?Filesystem $filesystem = null): FilesSink
    {
        return new FilesSink($filesystem ?? $this->fs, $destination, $this->saveMode());
    }
}
