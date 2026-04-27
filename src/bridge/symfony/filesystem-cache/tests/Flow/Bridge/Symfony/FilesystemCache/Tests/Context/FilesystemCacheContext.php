<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache\Tests\Context;

use Flow\Bridge\Symfony\FilesystemCache\FlowFilesystemCacheAdapter;
use Flow\Filesystem\{Filesystem, Path};
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;

final class FilesystemCacheContext
{
    public Path $directory;

    public string $directoryPath;

    public Filesystem $filesystem;

    public function __construct()
    {
        $this->filesystem = new NativeLocalFilesystem();
        $this->directoryPath = \sys_get_temp_dir() . '/flow-fs-cache-' . \bin2hex(\random_bytes(8));
        $this->directory = \Flow\Filesystem\DSL\path($this->directoryPath);
    }

    public function adapter(
        string $namespace = '',
        int $defaultLifetime = 0,
        ?MarshallerInterface $marshaller = null,
        ?Filesystem $filesystem = null,
    ) : FlowFilesystemCacheAdapter {
        return new FlowFilesystemCacheAdapter(
            $filesystem ?? $this->filesystem,
            $this->directory,
            $namespace,
            $defaultLifetime,
            $marshaller,
        );
    }

    public function cleanup() : void
    {
        if (\is_dir($this->directoryPath)) {
            $this->filesystem->rm(\Flow\Filesystem\DSL\path($this->directoryPath));
        }
    }

    /**
     * Overwrite the only existing cache file with a custom body.
     */
    public function corruptOnlyFile(string $body) : string
    {
        $path = $this->singleFilePath();

        if (\file_put_contents($path, $body) === false) {
            throw new \RuntimeException("Failed to write {$path}");
        }

        return $path;
    }

    /**
     * @return list<string>
     */
    public function listFiles() : array
    {
        if (!\is_dir($this->directoryPath)) {
            return [];
        }

        $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($this->directoryPath, \RecursiveDirectoryIterator::SKIP_DOTS));
        $files = [];

        foreach ($iterator as $entry) {
            if ($entry instanceof \SplFileInfo && $entry->isFile()) {
                $files[] = $entry->getPathname();
            }
        }

        \sort($files);

        return $files;
    }

    public function readFile(string $path) : string
    {
        $content = \file_get_contents($path);

        if ($content === false) {
            throw new \RuntimeException("Failed to read {$path}");
        }

        return $content;
    }

    public function readOnlyFile() : string
    {
        return $this->readFile($this->singleFilePath());
    }

    private function singleFilePath() : string
    {
        $files = $this->listFiles();

        if (\count($files) !== 1) {
            throw new \RuntimeException(\sprintf('Expected exactly 1 file under %s, found %d.', $this->directoryPath, \count($files)));
        }

        return $files[0];
    }
}
