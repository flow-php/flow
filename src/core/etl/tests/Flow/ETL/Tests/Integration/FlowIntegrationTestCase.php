<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\{Config\Cache\CacheConfig, Tests\FlowTestCase};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Filesystem\{FilesystemTable, Local\NativeLocalFilesystem, Local\StdOutFilesystem};
use Flow\Serializer\{Base64Serializer, NativePHPSerializer, Serializer};

abstract class FlowIntegrationTestCase extends FlowTestCase
{
    protected Path $cacheDir;

    protected Filesystem $fs;

    protected FilesystemTable $fstab;

    protected Serializer $serializer;

    private string|false $baseMemoryLimit;

    public function __construct(string $name)
    {
        parent::__construct($name);

        $this->baseMemoryLimit = (\ini_get('memory_limit')) ?: '-1';

        $this->cacheDir = Path::realpath(\getenv(CacheConfig::CACHE_DIR_ENV));
        $this->fs = new NativeLocalFilesystem();
        $this->fstab = new FilesystemTable($this->fs, new StdOutFilesystem());
        $this->serializer = new Base64Serializer(new NativePHPSerializer());
    }

    protected function setUp() : void
    {
        $this->cleanupCacheDir($this->cacheDir);
        \mkdir($this->cacheDir->path(), recursive: true);
    }

    protected function tearDown() : void
    {
        if (\ini_get('memory_limit') !== $this->baseMemoryLimit) {
            \ini_set('memory_limit', $this->baseMemoryLimit);
        }

        $this->cleanupCacheDir($this->cacheDir);
        \mkdir($this->cacheDir->path(), recursive: true);
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
        return $this->fs;
    }

    protected function fstab() : FilesystemTable
    {
        return $this->fstab;
    }

    protected function getPath(string $relativePath) : Path
    {
        return new Path($this->filesDirectory() . DIRECTORY_SEPARATOR . $relativePath);
    }

    protected function serializer() : Serializer
    {
        return $this->serializer;
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

    private function cleanupCacheDir(Path $path) : void
    {
        $this->fs()->rm($path);
    }
}
