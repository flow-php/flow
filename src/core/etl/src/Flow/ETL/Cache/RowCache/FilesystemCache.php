<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowCache;

use Flow\ETL\Cache\RowCache;
use Flow\ETL\{Exception\InvalidArgumentException, Hash\NativePHPHash, Row, Rows};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Serializer\{NativePHPSerializer, Serializer};

final class FilesystemCache implements RowCache
{
    private Path $cacheDir;

    /**
     * @param Filesystem $filesystem
     * @param Serializer $serializer
     * @param int<1, max> $chunkSize - number of rows to be written into cache in one go, higher number can reduce IO but increase memory consumption
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Serializer $serializer = new NativePHPSerializer(),
        private readonly int $chunkSize = 100,
        ?Path $cacheDir = null
    ) {
        if ($this->chunkSize < 1) {
            throw new InvalidArgumentException('Chunk size must be greater than 0');
        }

        $this->cacheDir = $cacheDir ?? $this->filesystem->getSystemTmpDir()->suffix('/flow-php-external-sort/');
    }

    /**
     * @return \Generator<Row>
     */
    public function get(string $key) : \Generator
    {
        $path = $this->keyPath($key);

        $stream = $this->filesystem->readFrom($path);

        foreach ($stream->readLines() as $serializedRow) {
            /** @phpstan-ignore-next-line */
            yield $this->serializer->unserialize($serializedRow, Row::class);
        }

        $stream->close();
    }

    public function remove(string $key) : void
    {
        // we want to remove not only cache file but entire directory
        $this->filesystem->rm($this->keyPath($key)->parentDirectory());
    }

    /**
     * @param string $key
     * @param iterable<Row>|Rows $rows
     */
    public function set(string $key, iterable $rows) : void
    {
        $path = $this->keyPath($key);

        $stream = $this->filesystem->writeTo($path);

        $serializedRows = '';
        $counter = 0;

        foreach ($rows as $row) {
            $serializedRows .= $this->serializer->serialize($row) . "\n";
            $counter++;

            if ($counter >= $this->chunkSize) {
                $stream->append($serializedRows);
                $serializedRows = '';
                $counter = 0;
            }
        }

        if ($counter > 0) {
            $stream->append($serializedRows);
        }

        $stream->close();
    }

    private function keyPath(string $key) : Path
    {
        return $this->cacheDir->suffix(NativePHPHash::xxh128($key) . '/rows.php.cache');
    }
}
