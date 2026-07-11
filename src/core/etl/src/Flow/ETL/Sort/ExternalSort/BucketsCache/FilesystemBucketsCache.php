<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort\BucketsCache;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Generator;

use function count;

final readonly class FilesystemBucketsCache implements BucketsCache
{
    private Path $cacheDir;

    private FloeReader $reader;

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private Filesystem $filesystem,
        ?Path $cacheDir = null,
        private int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        $this->cacheDir = ($cacheDir ?? $this->filesystem->getSystemTmpDir())->suffix('/flow-php-external-sort/');
        $this->reader = new FloeReader($this->filesystem);
    }

    /**
     * @return \Generator<Row>
     */
    public function get(string $bucketId): Generator
    {
        $path = $this->keyPath($bucketId);

        if (!$this->filesystem->status($path)) {
            return;
        }

        foreach ($this->reader->read($path)->recover($this->batchSize) as $batch) {
            yield from $batch->all();
        }
    }

    public function remove(string $bucketId): void
    {
        // we want to remove not only cache file but entire directory
        $this->filesystem->rm($this->keyPath($bucketId)->parentDirectory());
    }

    /**
     * @param string $bucketId
     * @param iterable<Row>|Rows $rows
     */
    public function set(string $bucketId, iterable $rows): void
    {
        $writer = new FloeWriter($this->filesystem);
        $writer->create($this->keyPath($bucketId));

        $batch = [];

        foreach ($rows as $row) {
            $batch[] = $row;

            if (count($batch) >= $this->batchSize) {
                $writer->write(new Rows(...$batch));
                $batch = [];
            }
        }

        if ($batch !== []) {
            $writer->write(new Rows(...$batch));
        }

        $writer->close();
    }

    private function keyPath(string $key): Path
    {
        return $this->cacheDir->suffix(NativePHPHash::xxh128($key) . '/' . $key . '.floe');
    }
}
