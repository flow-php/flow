<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition;

final class FilesystemStreams implements \Countable, \IteratorAggregate
{
    /**
     * @var array<string, FileStream>
     */
    private array $streams;

    public function __construct(private readonly Filesystem $filesystem)
    {
        $this->streams = [];
    }

    public function close() : void
    {
        foreach ($this->streams as $fileStream) {
            if ($fileStream->isOpen()) {
                $fileStream->close();
            }
        }

        $this->streams = [];
    }

    public function count() : int
    {
        return \count($this->streams);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function exists(Path $basePath, array $partitions = []) : bool
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        return $this->filesystem->exists($destination);
    }

    /**
     * @return \Traversable<string, FileStream>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator($this->streams);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function isOpen(Path $basePath, array $partitions = []) : bool
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        return \array_key_exists($destination->uri(), $this->streams);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function open(Path $basePath, string $extension, Mode $mode, bool $safe, array $partitions = []) : FileStream
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        if (\array_key_exists($destination->uri(), $this->streams)) {
            return $this->streams[$destination->uri()];
        }

        if ($destination->isPattern()) {
            throw new RuntimeException("Destination path can't be patter, given:" . $destination->uri());
        }

        if (!\array_key_exists($destination->uri(), $this->streams)) {
            $this->streams[$destination->uri()] = $this->filesystem->open(
                (\count($partitions) || $safe === true)  ? $destination->randomize()->setExtension($extension) : $basePath,
                $mode
            );
        }

        return $this->streams[$destination->uri()];
    }

    /**
     * @param Path $basePath
     * @param array<Partition> $partitions
     */
    public function rm(Path $basePath, array $partitions = []) : void
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        if ($this->filesystem->exists($destination)) {
            $this->filesystem->rm($destination);
        }
    }
}
