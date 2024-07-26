<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort\RowCache;

use Flow\ETL\Sort\ExternalSort\SortRowCache;
use Flow\ETL\{Exception\InvalidArgumentException, Hash\NativePHPHash, Row, Rows};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Serializer\{NativePHPSerializer, Serializer};

final class FilesystemSortRowCache implements SortRowCache
{
    /**
     * @param Filesystem $filesystem
     * @param Serializer $serializer
     * @param int<1, max> $chunkSize - number of rows to be written into cache in one go, higher number can reduce IO but increase memory consumption
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Serializer $serializer = new NativePHPSerializer(),
        private readonly int $chunkSize = 10,
    ) {
        if ($this->chunkSize < 1) {
            throw new InvalidArgumentException('Chunk size must be greater than 0');
        }
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
        $this->filesystem->rm($this->keyPath($key));
    }

    public function set(string $key, Rows $rows) : void
    {
        $path = $this->keyPath($key);

        $stream = $this->filesystem->writeTo($path);

        foreach ($rows->chunks($this->chunkSize) as $rowsChunk) {
            $serializedRows = '';

            foreach ($rowsChunk as $row) {
                $serializedRows .= $this->serializer->serialize($row) . "\n";
            }

            $stream->append($serializedRows);
        }

        $stream->close();
    }

    private function keyPath(string $key) : Path
    {
        return $this->filesystem->getSystemTmpDir()->suffix('/flow-php-external-sort/' . NativePHPHash::xxh128($key) . '/rows.php.cache');
    }
}
