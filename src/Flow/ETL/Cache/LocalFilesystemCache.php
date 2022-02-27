<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\Serializer\Serializer;

final class LocalFilesystemCache implements Cache
{
    private string $path;

    private Serializer $serializer;

    public function __construct(string $path, Serializer $serializer)
    {
        if (!\file_exists($path) || !\is_dir($path)) {
            throw new InvalidArgumentException("Given cache path does not exists or it's not a directory: {$path}");
        }

        $this->path = $path;
        $this->serializer = $serializer;
    }

    public function add(string $id, Rows $rows) : void
    {
        $cacheStream = \fopen($this->cachePath($id), 'a');

        if ($cacheStream === false) {
            throw new InvalidArgumentException("Failed to create cache file: \"{$this->cachePath($id)}\", mode \"a\"");
        }
        \fwrite($cacheStream, $this->serializer->serialize($rows) . "\n");
        \fclose($cacheStream);
    }

    /**
     * @param string $id
     *
     * @throws \Flow\ETL\Exception\RuntimeException
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function read(string $id) : \Generator
    {
        if (!\file_exists($cachePath = $this->cachePath($id))) {
            return;
        }

        /** @var resource $cacheStream */
        $cacheStream = \fopen($cachePath, 'r');

        while (($serializedRow = \fgets($cacheStream)) !== false) {
            /** @var Rows $rows */
            $rows = $this->serializer->unserialize($serializedRow);
            yield $rows;
        }

        \fclose($cacheStream);
    }

    public function clear(string $id) : void
    {
        if (!\file_exists($cachePath = $this->cachePath($id))) {
            return;
        }

        \unlink($cachePath);
    }

    /**
     * @param string $id
     *
     * @return string
     */
    private function cachePath(string $id) : string
    {
        return \rtrim($this->path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \hash('sha256', $id);
    }
}
