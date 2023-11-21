<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition;

/**
 * @psalm-suppress MissingTemplateParam
 */
final class FilesystemStreams implements \Countable, \IteratorAggregate
{
    private SaveMode $saveMode;

    /**
     * @var array<string, array<string, FileStream>>
     */
    private array $streams;

    public function __construct(private readonly Filesystem $filesystem)
    {
        $this->streams = [];
        $this->saveMode = SaveMode::ExceptionIfExists;
    }

    public function close(Path $basePath) : void
    {
        $streams = [];

        foreach ($this->streams as $nextBasePath => $nextStreams) {
            if ($basePath->uri() === $nextBasePath) {
                foreach ($nextStreams as $fileStream) {
                    if ($fileStream->isOpen()) {
                        $fileStream->close();
                    }
                }
            } else {
                $streams[$nextBasePath] = $nextStreams;
            }
        }

        $this->streams = $streams;
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

    public function fs() : Filesystem
    {
        return $this->filesystem;
    }

    /**
     * @return \Traversable<string, FileStream>
     */
    public function getIterator() : \Traversable
    {
        return new \ArrayIterator(\array_merge(...\array_values($this->streams)));
    }

    /**
     * @param array<Partition> $partitions
     */
    public function isOpen(Path $basePath, array $partitions = []) : bool
    {
        if (!\array_key_exists($basePath->uri(), $this->streams)) {
            return false;
        }

        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        return \array_key_exists($destination->uri(), $this->streams[$basePath->uri()]);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function open(Path $basePath, string $extension, bool $appendSafe, array $partitions = []) : FileStream
    {
        if (!\array_key_exists($basePath->uri(), $this->streams)) {
            $this->streams[$basePath->uri()] = [];
        }

        $destination = \count($partitions)
            ? $basePath->addPartitions(...$partitions)
            : $basePath;

        if (\array_key_exists($destination->uri(), $this->streams[$basePath->uri()])) {
            return $this->streams[$basePath->uri()][$destination->uri()];
        }

        if ($destination->isPattern()) {
            throw new RuntimeException("Destination path can't be patter, given:" . $destination->uri());
        }

        $path = (\count($partitions) || $appendSafe === true) ? $destination->randomize()->setExtension($extension) : $basePath;

        if (!\array_key_exists($destination->uri(), $this->streams[$basePath->uri()])) {
            if ($this->saveMode === SaveMode::Overwrite) {
                if ($this->filesystem->exists($destination)) {
                    $this->filesystem->rm($destination);
                }
            }

            if ($this->saveMode === SaveMode::ExceptionIfExists) {
                if ($this->filesystem->exists($destination)) {
                    throw new RuntimeException('Destination path "' . $destination->uri() . '" already exists, please change path to different or set different SaveMode');
                }
            }

            if ($this->saveMode === SaveMode::Append) {
                if (!$appendSafe) {
                    throw new RuntimeException('Appending to destination "' . $path->uri() . '" in non append safe mode is not supported.');
                }

                if ($this->filesystem->fileExists($destination) && !\count($partitions)) {
                    throw new RuntimeException('Appending to existing single file destination "' . $path->uri() . '" is not supported.');
                }
            }

            if ($this->saveMode === SaveMode::Ignore) {
                if ($this->filesystem->exists($destination)) {
                    $this->streams[$basePath->uri()][$destination->uri()] = FileStream::voidStream($path);
                } else {
                    $this->streams[$basePath->uri()][$destination->uri()] = $this->filesystem->open($path, Mode::WRITE_BINARY);
                }
            } else {
                $this->streams[$basePath->uri()][$destination->uri()] = $this->filesystem->open($path, Mode::WRITE_BINARY);
            }
        }

        return $this->streams[$basePath->uri()][$destination->uri()];
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

    public function setSaveMode(SaveMode $saveMode) : self
    {
        $this->saveMode = $saveMode;

        return $this;
    }
}
