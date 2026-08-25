<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\VoidStream;
use Generator;

use function array_key_exists;
use function count;
use function Flow\Filesystem\DSL\path;
use function str_contains;
use function str_replace;

final class FilesSink
{
    public const string FLOW_TMP_FILE_PREFIX = '._flow_php_tmp.';

    /**
     * Paths this state brought into existence, so abandon() can remove exactly those. A destination that already
     * existed and was left alone (Ignore) never lands here - removing it would delete the user's file.
     *
     * @var array<string, Path>
     */
    private array $created = [];

    /** @var array<string, DestinationStream> */
    private array $streams = [];

    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Path $destination,
        private readonly SaveMode $saveMode,
    ) {}

    public function abandon(): void
    {
        $streams = $this->streams;
        $created = $this->created;
        $this->streams = [];
        $this->created = [];

        // every handle closes first: a format writer flushes its footer on close, and the file has to be gone
        // after that, not before
        foreach ($streams as $stream) {
            if ($stream->isOpen()) {
                $stream->close();
            }
        }

        foreach ($created as $path) {
            if ($this->filesystem->status($path) !== null) {
                $this->filesystem->rm($path);
            }
        }
    }

    public function publish(): void
    {
        $streams = $this->streams;
        $this->streams = [];
        $this->created = [];

        foreach ($streams as $stream) {
            if ($stream->isOpen()) {
                $stream->close();
            }

            if ($this->saveMode !== SaveMode::Overwrite) {
                continue;
            }

            if ($stream->path()->partitions()->count() || [] !== $this->destination->partitionPlaceholders()) {
                $writtenFiles = path(
                    $stream->path()->parentDirectory()->uri()
                        . '/'
                        . str_replace(self::FLOW_TMP_FILE_PREFIX, '', $stream->path()->filename())
                        . '*.'
                        // @mago-ignore analysis:possibly-false-operand
                        . $stream->path()->extension(),
                    $stream->path()->options(),
                );

                foreach ($this->filesystem->list($writtenFiles) as $stale) {
                    if (str_contains($stale->path->path(), self::FLOW_TMP_FILE_PREFIX)) {
                        continue;
                    }

                    $this->filesystem->rm($stale->path);
                }
            }

            $this->filesystem->mv($stream->path(), path(
                str_replace(self::FLOW_TMP_FILE_PREFIX, '', $stream->path()->uri()),
                $stream->path()->options(),
            ));
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function touched(array $partitions = []): bool
    {
        return array_key_exists(
            count($partitions) ? $this->destination->addPartitions(...$partitions)->uri() : $this->destination->uri(),
            $this->streams,
        );
    }

    /**
     * @return \Generator<DestinationStream>
     */
    public function openStreams(): Generator
    {
        foreach ($this->streams as $stream) {
            if ($stream->isOpen()) {
                yield $stream;
            }
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function writeTo(array $partitions = []): DestinationStream
    {
        if (!$this->destination->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $this->destination->uri());
        }

        $placeholders = $this->destination->partitionPlaceholders();

        if ($this->destination->isPattern() && [] === $placeholders) {
            throw new RuntimeException("Destination path can't be pattern, given: " . $this->destination->uri());
        }

        if ([] !== $placeholders && !count($partitions)) {
            throw new RuntimeException(
                'Destination path "'
                . $this->destination->uri()
                . '" contains partition placeholders but rows are not partitioned, add partitionBy() to your pipeline',
            );
        }

        $destination = count($partitions) ? $this->destination->addPartitions(...$partitions) : $this->destination;
        $uri = $destination->uri();

        if (array_key_exists($uri, $this->streams)) {
            return $this->streams[$uri];
        }

        $exists = $this->filesystem->status($destination) !== null;

        if ($this->saveMode === SaveMode::ExceptionIfExists && $exists) {
            throw new RuntimeException(
                'Destination path "'
                . $uri
                . '" already exists, please change path to different or set different SaveMode',
            );
        }

        if ($this->saveMode === SaveMode::Ignore && $exists) {
            return $this->streams[$uri] = new VoidStream($destination);
        }

        $outputPath = match (true) {
            $this->saveMode === SaveMode::Append && $exists => $destination->randomize(),
            $this->saveMode === SaveMode::Overwrite => $destination->basenamePrefix(self::FLOW_TMP_FILE_PREFIX),
            default => $destination,
        };

        $this->created[$uri] = $outputPath;

        return $this->streams[$uri] = $this->filesystem->writeTo($outputPath);
    }
}
