<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected string $cacheDir;

    protected Filesystem $fs;

    private string $baseMemoryLimit;

    protected function setUp() : void
    {
        $this->baseMemoryLimit = \ini_get('memory_limit');
        $this->cacheDir = Path::realpath(\getenv(Config::CACHE_DIR_ENV))->path();

        $this->fs = new LocalFilesystem();

        $this->cleanupCacheDir($this->cacheDir);

        if (!$this->fs->directoryExists(Path::realpath($this->cacheDir))) {
            \mkdir($this->cacheDir, recursive: true);
        }
    }

    protected function tearDown() : void
    {
        if (\ini_get('memory_limit') !== $this->baseMemoryLimit) {
            \ini_set('memory_limit', $this->baseMemoryLimit);
        }

        $this->cleanupCacheDir($this->cacheDir);
    }

    private function cleanupCacheDir(string $directory) : void
    {
        if ($this->fs->directoryExists($path = Path::realpath($directory))) {
            $this->fs->rm($path);
        }
    }
}
