<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Filesystem\Path;
use League\Flysystem\Filesystem;
use League\Flysystem\Local\LocalFilesystemAdapter;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected string $cacheDir;

    private string $baseMemoryLimit;

    private Filesystem $fs;

    protected function setUp() : void
    {
        $this->baseMemoryLimit = \ini_get('memory_limit');
        $this->cacheDir = Path::realpath(\getenv(Config::CACHE_DIR_ENV))->path();

        $this->fs = new Filesystem(new LocalFilesystemAdapter(DIRECTORY_SEPARATOR));

        $this->fs->directoryExists($this->cacheDir);

        if (!$this->fs->directoryExists($this->cacheDir)) {
            $this->fs->createDirectory($this->cacheDir);
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
        if ($this->fs->directoryExists($directory)) {
            $this->fs->deleteDirectory($directory);
        }
    }
}
