<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;

/**
 * @implements Cache<array{path: string, serializer: Serializer}>
 * @infection-ignore-all
 */
final class LocalFilesystemCache implements Cache
{
    public function __construct(
        private readonly string $path,
        private readonly Serializer $serializer = new NativePHPSerializer()
    ) {
        if (!\file_exists($path) || !\is_dir($path)) {
            throw new InvalidArgumentException("Given cache path does not exists or it's not a directory: {$path}");
        }
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'serializer' => $this->serializer,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->serializer = $data['serializer'];
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

    public function clear(string $id) : void
    {
        if (!\file_exists($cachePath = $this->cachePath($id))) {
            return;
        }

        \unlink($cachePath);
    }

    /**
     * @throws \Flow\ETL\Exception\RuntimeException
     *
     * @return \Generator<Rows>
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

    private function cachePath(string $id) : string
    {
        return \rtrim($this->path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . \hash('sha256', $id);
    }
}
