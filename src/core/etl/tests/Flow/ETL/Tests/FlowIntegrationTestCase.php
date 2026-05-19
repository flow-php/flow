<?php

declare(strict_types=1);

namespace Flow\ETL\Tests;

use Flow\ETL\Config\Cache\CacheConfig;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Local\StdOutFilesystem;
use Flow\Filesystem\Path;
use Flow\Serializer\Base64Serializer;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;
use RuntimeException;

use function count;
use function file_put_contents;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function getenv;
use function in_array;
use function ini_get;
use function ini_set;
use function is_string;
use function mkdir;
use function scandir;

/**
 * Test case for integration tests.
 */
abstract class FlowIntegrationTestCase extends FlowTestCase
{
    protected Path $cacheDir;

    protected Filesystem $fs;

    protected FilesystemTable $fstab;

    protected Serializer $serializer;

    private string $baseMemoryLimit;

    protected function setUp(): void
    {
        $this->baseMemoryLimit = ini_get('memory_limit') ?: '-1';

        $cacheDirEnv = getenv(CacheConfig::CACHE_DIR_ENV);
        $this->cacheDir = path_real($cacheDirEnv ?: '');
        $this->fs = new NativeLocalFilesystem();
        $this->fstab = new FilesystemTable($this->fs, new StdOutFilesystem());
        $this->serializer = new Base64Serializer(new NativePHPSerializer());

        $this->cleanupCacheDir($this->cacheDir);
        mkdir($this->cacheDir->path(), recursive: true);
    }

    protected function tearDown(): void
    {
        if (ini_get('memory_limit') !== $this->baseMemoryLimit) {
            ini_set('memory_limit', $this->baseMemoryLimit);
        }

        $this->cleanupCacheDir($this->cacheDir);
        mkdir($this->cacheDir->path(), recursive: true);
    }

    protected function cleanFiles(): void
    {
        $files = scandir($this->filesDirectory());

        if ($files === false) {
            return;
        }

        foreach ($files as $file) {
            if (in_array($file, ['.', '..', '.gitignore'], true)) {
                continue;
            }

            $this->fs()->rm(path_real($this->filesDirectory() . DIRECTORY_SEPARATOR . $file));
        }
    }

    protected function filesDirectory(): string
    {
        throw new RuntimeException(
            'You need to implement filesDirectory method to point to your test files directory.',
        );
    }

    protected function fs(): Filesystem
    {
        return $this->fs;
    }

    protected function fstab(): FilesystemTable
    {
        return $this->fstab;
    }

    protected function getPath(string $relativePath): Path
    {
        return path($this->filesDirectory() . DIRECTORY_SEPARATOR . $relativePath);
    }

    protected function serializer(): Serializer
    {
        return $this->serializer;
    }

    /**
     * @param array<string, array<string, mixed>|string> $datasets
     */
    protected function setupFiles(array $datasets, string $path = ''): void
    {
        foreach ($datasets as $name => $content) {
            if (is_string($content)) {
                $result = file_put_contents(
                    $this->filesDirectory() . DIRECTORY_SEPARATOR . $path . DIRECTORY_SEPARATOR . $name,
                    $content,
                );

                if ($result === false) {
                    throw new RuntimeException('Could not create file . ' . $name);
                }

                continue;
            }

            mkdir($this->filesDirectory() . DIRECTORY_SEPARATOR . $path . DIRECTORY_SEPARATOR . $name, recursive: true);

            if (count($content)) {
                /** @var array<string,string> $content */
                $this->setupFiles($content, $path . DIRECTORY_SEPARATOR . $name);
            }
        }
    }

    private function cleanupCacheDir(Path $path): void
    {
        $this->fs()->rm($path);
    }
}
