<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\Cache\LocalFilesystemCache;
use PHPUnit\Framework\TestCase;

abstract class CacheTestCase extends TestCase
{
    protected string $cacheDir;

    protected function setUp() : void
    {
        $this->cacheDir = \getenv(LocalFilesystemCache::CACHE_DIR_ENV);

        if (!\file_exists($this->cacheDir)) {
            \mkdir($this->cacheDir);
        }

        $this->cleanupCacheDir($this->cacheDir);
    }

    protected function tearDown() : void
    {
        $this->cleanupCacheDir($this->cacheDir);
    }

    private function cleanupCacheDir(string $directory) : void
    {
        if (\file_exists($directory)) {
            foreach (\array_diff(\scandir($directory), ['.', '..']) as $fileName) {
                $filePath = $directory . DIRECTORY_SEPARATOR . $fileName;

                if (\is_file($filePath)) {
                    \unlink($filePath);
                }
            }
        }
    }
}
