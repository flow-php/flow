<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\RowsValueMapper;
use Generator;

use function count;
use function str_split;
use function substr;

final readonly class FilesystemCache implements Cache
{
    private Path $cacheDir;

    private FloeReader $reader;

    /**
     * @param int<1, max> $serializerBatchSize
     */
    public function __construct(
        private Filesystem $filesystem,
        ?Path $cacheDir = null,
        private int $serializerBatchSize = 1000,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->serializerBatchSize < 1) {
            throw new InvalidArgumentException('Serializer batch size must be at least 1');
        }

        $this->cacheDir = $cacheDir ?? $this->filesystem->getSystemTmpDir();
        $this->reader = new FloeReader($this->filesystem);
    }

    public function clear(): void
    {
        $this->filesystem->rm($this->cacheDir);
    }

    public function delete(string $key): void
    {
        $this->filesystem->rm($this->cachePath($key));
    }

    public function get(string $key): Rows
    {
        $path = $this->cachePath($key);

        if (!$this->filesystem->status($path)) {
            throw new KeyNotInCacheException($key);
        }

        $file = $this->reader->read($path);

        try {
            $footer = $file->footer();
            $rows = [];

            foreach ($file->recover($this->serializerBatchSize) as $batch) {
                foreach ($batch->all() as $row) {
                    $rows[] = $row;
                }
            }

            if (count($rows) !== $footer->totalRows) {
                throw new KeyNotInCacheException($key);
            }

            return RowsValueMapper::wrap(RowsValueMapper::reconstructFrom($rows, $footer));
        } catch (FloeException $e) {
            throw new KeyNotInCacheException($key, $e);
        }
    }

    public function has(string $key): bool
    {
        return $this->filesystem->status($this->cachePath($key)) !== null;
    }

    public function read(string $key): Generator
    {
        $path = $this->cachePath($key);

        if (!$this->filesystem->status($path)) {
            throw new KeyNotInCacheException($key);
        }

        $file = $this->reader->read($path);

        try {
            $partitions = RowsValueMapper::partitionsFrom($file->footer());
        } catch (FloeException $e) {
            throw new KeyNotInCacheException($key, $e);
        }

        foreach ($file->recover($this->serializerBatchSize) as $batch) {
            // recover() attaches partitions in on-wire order; the caller-visible order is footer metadata
            yield $partitions === [] ? $batch : Rows::partitioned($batch->all(), $partitions);
        }
    }

    public function set(string $key, Rows $value): void
    {
        $writer = new FloeWriter($this->filesystem);
        $writer->create($this->cachePath($key), RowsValueMapper::metadataFor($value));

        // an empty Rows still crosses once - chunks() would yield nothing and an
        // empty-but-partitioned value would lose its PARTITIONS frame (byte parity)
        foreach ($value->count() === 0 ? [$value] : $value->chunks($this->serializerBatchSize) as $chunk) {
            $writer->write($chunk);
        }

        $writer->close();
    }

    private function cachePath(string $key): Path
    {
        return $this->cacheDir->suffix(
            implode('/', str_split(substr(NativePHPHash::xxh128($key), 0, 8), 2)) . '/' . $key . '.floe',
        );
    }
}
