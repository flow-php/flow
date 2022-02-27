<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\Config;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected string $cacheDir;

    private string $baseMemoryLimit;

    protected function setUp() : void
    {
        $this->baseMemoryLimit = \ini_get('memory_limit');
        $this->cacheDir = \getenv(Config::CACHE_DIR_ENV);

        if (!\file_exists($this->cacheDir)) {
            \mkdir($this->cacheDir);
        }

        $this->cleanupCacheDir($this->cacheDir);
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
