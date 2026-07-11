<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use ArrayIterator;
use Countable;
use Flow\ETL\Exception\RuntimeException;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\PlaceholderPartitions;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\VoidStream;
use Generator;
use IteratorAggregate;
use Traversable;

use function array_key_exists;
use function array_merge;
use function array_values;
use function count;
use function Flow\Filesystem\DSL\path;
use function str_contains;
use function str_replace;

/**
 * @implements \IteratorAggregate<array-key, DestinationStream>
 */
final class FilesystemStreams implements Countable, IteratorAggregate
{
    public const string FLOW_TMP_FILE_PREFIX = '._flow_php_tmp.';

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    /**
     * @var array<string, array<string, DestinationStream>>
     */
    private array $writingStreams = [];

    public function __construct(
        private readonly FilesystemTable $fstab,
    ) {}

    public function closeStreams(Path $path): void
    {
        $streams = [];

        $fs = $this->fstab->for($path);

        foreach ($this->writingStreams as $nextBasePath => $nextStreams) {
            if ($path->uri() === $nextBasePath) {
                foreach ($nextStreams as $fileStream) {
                    if ($fileStream->isOpen()) {
                        $fileStream->close();
                    }

                    if ($this->saveMode === SaveMode::Overwrite) {
                        if ($fileStream->path()->partitions()->count() || [] !== $path->partitionPlaceholders()) {
                            $filename = str_replace(self::FLOW_TMP_FILE_PREFIX, '', $fileStream->path()->filename());

                            $partitionFilesPattern = path(
                                $fileStream->path()->parentDirectory()->uri() . '/' . $filename . '*.'
                                    // @mago-ignore analysis:possibly-false-operand
                                    . $fileStream->path()->extension(),
                                $fileStream->path()->options(),
                            );

                            foreach ($fs->list($partitionFilesPattern) as $partitionFile) {
                                if (str_contains($partitionFile->path->path(), self::FLOW_TMP_FILE_PREFIX)) {
                                    continue;
                                }

                                $fs->rm($partitionFile->path);
                            }
                        }

                        $fs->mv($fileStream->path(), path(
                            str_replace(self::FLOW_TMP_FILE_PREFIX, '', $fileStream->path()->uri()),
                            $fileStream->path()->options(),
                        ));
                    }
                }
            } else {
                $streams[$nextBasePath] = $nextStreams;
            }
        }

        $this->writingStreams = $streams;
    }

    public function count(): int
    {
        return count($this->writingStreams);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function exists(Path $path, array $partitions = []): bool
    {
        $destination = count($partitions) ? $path->addPartitions(...$partitions) : $path;

        return $this->fstab->for($path)->status($destination) !== null;
    }

    /**
     * @return \Traversable<string, DestinationStream>
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator(array_merge(...array_values($this->writingStreams)));
    }

    /**
     * @param array<Partition> $partitions
     */
    public function isOpen(Path $path, array $partitions = []): bool
    {
        if (!array_key_exists($path->uri(), $this->writingStreams)) {
            return false;
        }

        $destination = count($partitions) ? $path->addPartitions(...$partitions) : $path;

        return array_key_exists($destination->uri(), $this->writingStreams[$path->uri()]);
    }

    /**
     * @return \Generator<SourceStream>
     */
    public function list(Path $path, Filter $pathFilter): Generator
    {
        $fs = $this->fstab->for($path);
        $hasPlaceholders = [] !== $path->partitionPlaceholders();

        if ($hasPlaceholders) {
            $pathFilter = new PlaceholderPartitions($path, $pathFilter);
        }

        foreach ($fs->list($path, $pathFilter) as $file) {
            yield $fs->readFrom(
                $hasPlaceholders
                    ? $file->path->withPartitions($path->extractPlaceholderPartitions($file->path))
                    : $file->path,
            );
        }
    }

    /**
     * @return \Generator<DestinationStream>
     */
    public function listOpenStreams(Path $path): Generator
    {
        $uri = $path->uri();

        if (!array_key_exists($uri, $this->writingStreams)) {
            return;
        }

        foreach ($this->writingStreams[$uri] as $stream) {
            if ($stream->isOpen()) {
                yield $stream;
            }
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function read(Path $path, array $partitions = []): SourceStream
    {
        $placeholders = $path->partitionPlaceholders();

        if ($path->isPattern() && [] === $placeholders) {
            throw new RuntimeException("Path can't be pattern, given: " . $path->uri());
        }

        if ([] !== $placeholders && !count($partitions)) {
            throw new RuntimeException(
                'Path "' . $path->uri() . '" contains partition placeholders but no partitions were given',
            );
        }

        $destination = count($partitions) ? $path->addPartitions(...$partitions) : $path;

        return $this->fstab->for($path)->readFrom($destination);
    }

    /**
     * @param Path $path
     * @param array<Partition> $partitions
     */
    public function rm(Path $path, array $partitions = []): void
    {
        $destination = count($partitions) ? $path->addPartitions(...$partitions) : $path;

        $fs = $this->fstab->for($path);

        if ($fs->status($destination)) {
            $fs->rm($destination);
        }
    }

    public function setMode(SaveMode $saveMode): self
    {
        $this->saveMode = $saveMode;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function writeTo(Path $path, array $partitions = []): DestinationStream
    {
        if (!$path->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $path->uri());
        }

        $placeholders = $path->partitionPlaceholders();

        if ($path->isPattern() && [] === $placeholders) {
            throw new RuntimeException("Destination path can't be pattern, given: " . $path->uri());
        }

        if ([] !== $placeholders && !count($partitions)) {
            throw new RuntimeException(
                'Destination path "'
                . $path->uri()
                . '" contains partition placeholders but rows are not partitioned, add partitionBy() to your pipeline',
            );
        }

        $pathUri = $path->uri();

        if (!array_key_exists($pathUri, $this->writingStreams)) {
            $this->writingStreams[$pathUri] = [];
        }

        $destination = count($partitions) ? $path->addPartitions(...$partitions) : $path;

        $destinationPathUri = $destination->uri();

        if (!array_key_exists($destinationPathUri, $this->writingStreams[$pathUri])) {
            $fs = $this->fstab->for($path);

            $outputPath = $destination;

            if ($this->saveMode === SaveMode::Append) {
                if ($fs->status($outputPath) !== null) {
                    $outputPath = $outputPath->randomize();
                }
            }

            if ($this->saveMode === SaveMode::Overwrite) {
                $outputPath = $outputPath->basenamePrefix(self::FLOW_TMP_FILE_PREFIX);
            }

            if ($this->saveMode === SaveMode::ExceptionIfExists) {
                if ($fs->status($destination)) {
                    throw new RuntimeException(
                        'Destination path "'
                        . $destinationPathUri
                        . '" already exists, please change path to different or set different SaveMode',
                    );
                }
            }

            if ($this->saveMode === SaveMode::Ignore) {
                if ($fs->status($destination)) {
                    return $this->writingStreams[$pathUri][$destinationPathUri] = new VoidStream($outputPath);
                }
            }

            return $this->writingStreams[$pathUri][$destinationPathUri] = $fs->writeTo($outputPath);
        }

        return $this->writingStreams[$pathUri][$destinationPathUri];
    }
}
