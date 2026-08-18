<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing\Storage;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Options;
use Generator;

final class FilesystemBuckets implements BucketsStorage
{
    private readonly Path $cacheDir;

    private readonly FloeReader $reader;

    /**
     * @var array<string, FloeWriter>
     */
    private array $writers = [];

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        Path $cacheDir,
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        $this->cacheDir = $cacheDir->suffix('/flow-php-buckets/');
        $this->reader = new FloeReader($this->filesystem);
    }

    public function append(string $bucketId, Rows $rows): void
    {
        if (!isset($this->writers[$bucketId])) {
            $writer = new FloeWriter($this->filesystem, $rows->schema(), new Options(validateData: true));
            $writer->append($this->keyPath($bucketId));
            $this->writers[$bucketId] = $writer;
        }

        $this->writers[$bucketId]->write($rows);
    }

    public function get(string $bucketId): Generator
    {
        $this->closeWriter($bucketId);

        $path = $this->keyPath($bucketId);

        if (!$this->filesystem->status($path)) {
            return;
        }

        $reader = $this->reader->read($path);

        // finally, not a trailing close(): PHP runs it on generator destruction too, and a KWayMerge cursor is
        // destroyed rather than exhausted when a merge throws
        try {
            foreach ($reader->rows($this->batchSize, conform: false) as $batch) {
                yield $batch;
            }
        } finally {
            $reader->close();
        }
    }

    public function remove(string $bucketId): void
    {
        $this->closeWriter($bucketId);

        // we want to remove not only cache file but entire directory
        $this->filesystem->rm($this->keyPath($bucketId)->parentDirectory());
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->closeWriter($bucketId);

        // the session schema is derived from these exact rows, so validation could only compare them to themselves
        $writer = new FloeWriter($this->filesystem, $rows->schema(), new Options(validateData: false));
        $writer->create($this->keyPath($bucketId));
        $writer->write($rows);
        $writer->close();
    }

    private function closeWriter(string $bucketId): void
    {
        if (!isset($this->writers[$bucketId])) {
            return;
        }

        $writer = $this->writers[$bucketId];
        unset($this->writers[$bucketId]);

        $writer->close();
    }

    private function keyPath(string $key): Path
    {
        return $this->cacheDir->suffix(NativePHPHash::xxh128($key) . '/' . $key . '.floe');
    }
}
