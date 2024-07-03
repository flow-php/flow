<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\{Config};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Filesystem\{FilesystemTable, Local\NativeLocalFilesystem};
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected string $cacheDir = __DIR__ . '/var/';

    protected ?Filesystem $fs = null;

    protected ?FilesystemTable $fstab = null;

    private string $baseMemoryLimit = '-1';

    protected function setUp() : void
    {
        $this->baseMemoryLimit = \ini_get('memory_limit');
        $this->cacheDir = Path::realpath(\getenv(Config::CACHE_DIR_ENV))->path();

        $this->cleanupCacheDir($this->cacheDir);

        if (!$this->fs()->status(Path::realpath($this->cacheDir))?->isDirectory()) {
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

    protected function cleanFiles() : void
    {
        foreach (\scandir($this->filesDirectory()) as $file) {
            if (\in_array($file, ['.', '..', '.gitignore'], true)) {
                continue;
            }

            $this->fs()->rm(Path::realpath($this->filesDirectory() . DIRECTORY_SEPARATOR . $file));
        }
    }

    protected function filesDirectory() : string
    {
        throw new \RuntimeException('You need to implement filesDirectory method to point to your test files directory.');
    }

    protected function fs() : Filesystem
    {
        if ($this->fs === null) {
            $this->fs = new NativeLocalFilesystem();
        }

        return $this->fs;
    }

    protected function fstab() : FilesystemTable
    {
        if ($this->fstab === null) {
            $this->fstab = new FilesystemTable($this->fs());
        }

        return $this->fstab;
    }

    protected function getPath(string $relativePath) : Path
    {
        return new Path($this->filesDirectory() . DIRECTORY_SEPARATOR . $relativePath);
    }

    /**
     * @param array<string, array<string, string>|string> $datasets
     */
    protected function setupFiles(array $datasets, $path = '') : void
    {
        foreach ($datasets as $name => $content) {
            if (\is_string($content)) {
                $result = \file_put_contents($this->filesDirectory() . DIRECTORY_SEPARATOR . $path . DIRECTORY_SEPARATOR . $name, $content);

                if ($result === false) {
                    throw new \RuntimeException('Could not create file . ' . $name);
                }

                continue;
            }

            \mkdir($this->filesDirectory() . DIRECTORY_SEPARATOR . $path . DIRECTORY_SEPARATOR . $name, recursive: true);

            if (\count($content)) {
                /** @var array<string,string> $content */
                $this->setupFiles($content, $path . DIRECTORY_SEPARATOR . $name);
            }
        }
    }

    private function cleanupCacheDir(string $directory) : void
    {
        if ($this->fs()->status($path = Path::realpath($directory))?->isDirectory()) {
            $this->fs()->rm($path);
        }
    }
}
