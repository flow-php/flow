<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem\FilesystemStreams;

use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

abstract class FilesystemStreamsTestCase extends FlowIntegrationTestCase
{
    protected function filesDirectory() : string
    {
        return __DIR__ . '/tmp';
    }

    abstract protected function streams() : FilesystemStreams;
}
