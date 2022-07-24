<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

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
     * @param Path $basePath
     * @param array<Partition> $partitions
     *
     * @return bool
     */
    public function exists(Path $basePath, array $partitions = []) : bool
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        return \array_key_exists($destination->uri(), $this->streams);
    }

    /**
     * @return \Traversable<string, FileStream>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator($this->streams);
    }

    /**
     * @param Path $basePath
     * @param string $extension
     * @param Mode $mode
     * @param bool $safe
     * @param array<Partition> $partitions
     *
     * @return FileStream
     */
    public function open(Path $basePath, string $extension, Mode $mode, bool $safe, array $partitions = []) : FileStream
    {
        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        if (!\array_key_exists($destination->uri(), $this->streams)) {
            $this->streams[$destination->uri()] = $this->filesystem->open(
                (\count($partitions) || $safe === true)  ? $destination->randomize()->setExtension($extension) : $basePath,
                $mode
            );
        }

        return $this->streams[$destination->uri()];
    }
}
