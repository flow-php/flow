<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\Filesystem\{DestinationStream, Path, Path\Filter, SourceStream, Stream\VoidStream};
use Flow\Filesystem\{FilesystemTable, Partition};

/**
 * @implements \IteratorAggregate<array-key, DestinationStream>
 */
final class FilesystemStreams implements \Countable, \IteratorAggregate
{
    public const FLOW_TMP_FILE_PREFIX = '._flow_php_tmp.';

    private SaveMode $saveMode;

    /**
     * @var array<string, array<string, DestinationStream>>
     */
    private array $writingStreams;

    public function __construct(private readonly FilesystemTable $fstab)
    {
        $this->writingStreams = [];
        $this->saveMode = SaveMode::ExceptionIfExists;
    }

    public function closeStreams(Path $path) : void
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
                        if ($fileStream->path()->partitions()->count()) {
                            $partitionFilesPatter = new Path($fileStream->path()->parentDirectory()->path() . '/*', $fileStream->path()->options());

                            foreach ($fs->list($partitionFilesPatter) as $partitionFile) {
                                if (\str_contains($partitionFile->path->path(), self::FLOW_TMP_FILE_PREFIX)) {
                                    continue;
                                }

                                $fs->rm($partitionFile->path);
                            }
                        }

                        $fs->mv(
                            $fileStream->path(),
                            new Path(
                                \str_replace(self::FLOW_TMP_FILE_PREFIX, '', $fileStream->path()->uri()),
                                $fileStream->path()->options()
                            )
                        );
                    }
                }
            } else {
                $streams[$nextBasePath] = $nextStreams;
            }
        }

        $this->writingStreams = $streams;
    }

    public function count() : int
    {
        return \count($this->writingStreams);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function exists(Path $path, array $partitions = []) : bool
    {
        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        return $this->fstab->for($path)->status($destination) !== null;
    }

    /**
     * @return \Traversable<string, DestinationStream>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator(\array_merge(...\array_values($this->writingStreams)));
    }

    /**
     * @param array<Partition> $partitions
     */
    public function isOpen(Path $path, array $partitions = []) : bool
    {
        if (!\array_key_exists($path->uri(), $this->writingStreams)) {
            return false;
        }

        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        return \array_key_exists($destination->uri(), $this->writingStreams[$path->uri()]);
    }

    /**
     * @return \Generator<SourceStream>
     */
    public function list(Path $path, Filter $pathFilter) : \Generator
    {
        $fs = $this->fstab->for($path);

        foreach ($fs->list($path, $pathFilter) as $file) {
            yield $fs->readFrom($file->path);
        }
    }

    public function listOpenStreams(Path $path) : \Generator
    {
        $uri = $path->uri();

        if (!\array_key_exists($uri, $this->writingStreams)) {
            return [];
        }

        foreach ($this->writingStreams[$uri] as $stream) {
            if ($stream->isOpen()) {
                yield $stream;
            }
        }
    }

    public function read(Path $path, array $partitions = []) : SourceStream
    {
        if ($path->isPattern()) {
            throw new RuntimeException("Path can't be pattern, given: " . $path->uri());
        }

        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        return $this->fstab->for($path)->readFrom($destination);
    }

    /**
     * @param Path $path
     * @param array<Partition> $partitions
     */
    public function rm(Path $path, array $partitions = []) : void
    {
        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        $fs = $this->fstab->for($path);

        if ($fs->status($destination)) {
            $fs->rm($destination);
        }
    }

    public function setSaveMode(SaveMode $saveMode) : self
    {
        $this->saveMode = $saveMode;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function writeTo(Path $path, array $partitions = []) : DestinationStream
    {
        if (!$path->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $path->uri());
        }

        if ($path->isPattern()) {
            throw new RuntimeException("Destination path can't be patter, given:" . $path->uri());
        }

        $pathUri = $path->uri();

        if (!\array_key_exists($pathUri, $this->writingStreams)) {
            $this->writingStreams[$pathUri] = [];
        }

        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        $destinationPathUri = $destination->uri();

        if (!\array_key_exists($destinationPathUri, $this->writingStreams[$pathUri])) {
            if ($path->protocol()->is('stdout') && \count($this->writingStreams) > 0) {
                foreach ($this->getIterator() as $writingStream) {
                    if ($writingStream->path()->protocol()->is('stdout')) {
                        throw new RuntimeException('Only one stdout filesystem stream can be open at the same time');
                    }
                }
            }

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
                    throw new RuntimeException('Destination path "' . $destinationPathUri . '" already exists, please change path to different or set different SaveMode');
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
