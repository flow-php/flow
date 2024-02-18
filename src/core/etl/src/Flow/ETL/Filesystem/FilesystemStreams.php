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
    public const FLOW_TMP_SUFFIX = '._flow_tmp';

    private SaveMode $saveMode;

    /**
     * @var array<string, array<string, FileStream>>
     */
    private array $writingStreams;

    public function __construct(private readonly Filesystem $filesystem)
    {
        $this->writingStreams = [];
        $this->saveMode = SaveMode::ExceptionIfExists;
    }

    public function closeWriters(Path $path) : void
    {
        $streams = [];

        foreach ($this->writingStreams as $nextBasePath => $nextStreams) {
            if ($path->uri() === $nextBasePath) {
                foreach ($nextStreams as $fileStream) {

                    if ($fileStream->isOpen()) {
                        $fileStream->close();
                    }

                    if ($this->saveMode === SaveMode::Overwrite) {
                        if ($fileStream->path()->partitions()->count()) {
                            $partitionFilesPatter = new Path($fileStream->path()->parentDirectory()->path() . '/*', $fileStream->path()->options());

                            foreach ($this->filesystem->scan($partitionFilesPatter) as $partitionFile) {
                                if (\str_ends_with($partitionFile->path(), self::FLOW_TMP_SUFFIX)) {
                                    continue;
                                }

                                $this->filesystem->rm($partitionFile);
                            }
                        }

                        $this->filesystem->mv(
                            $fileStream->path(),
                            new Path(
                                \str_replace(self::FLOW_TMP_SUFFIX, '', $fileStream->path()->uri()),
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

        return $this->filesystem->exists($destination);
    }

    /**
     * @return \Traversable<string, FileStream>
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

    public function read(Path $path, array $partitions = []) : FileStream
    {
        if ($path->isPattern()) {
            throw new RuntimeException("Path can't be pattern, given: " . $path->uri());
        }

        $destination = \count($partitions)
            ? $path->addPartitions(...$partitions)
            : $path;

        return $this->filesystem->open($destination, Mode::READ);
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

        if ($this->filesystem->exists($destination)) {
            $this->filesystem->rm($destination);
        }
    }

    /**
     * @return \Generator<FileStream>
     */
    public function scan(Path $path, Partition\PartitionFilter $partitionFilter) : \Generator
    {
        foreach ($this->filesystem->scan($path, $partitionFilter) as $file) {
            yield $this->filesystem->open($file, Mode::READ);
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
    public function writeTo(Path $path, array $partitions = []) : FileStream
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
            $outputPath = $destination;

            if ($this->saveMode === SaveMode::Append) {
                if ($this->filesystem->fileExists($outputPath)) {
                    $outputPath = $outputPath->randomize();
                }
            }

            if ($this->saveMode === SaveMode::Overwrite) {
                $outputPath = new Path($outputPath->uri() . self::FLOW_TMP_SUFFIX, $outputPath->options());
            }

            if ($this->saveMode === SaveMode::ExceptionIfExists) {
                if ($this->filesystem->exists($destination)) {
                    throw new RuntimeException('Destination path "' . $destinationPathUri . '" already exists, please change path to different or set different SaveMode');
                }
            }

            if ($this->saveMode === SaveMode::Ignore) {
                if ($this->filesystem->exists($destination)) {
                    return $this->writingStreams[$pathUri][$destinationPathUri] = FileStream::voidStream($outputPath);
                }
            }

            return $this->writingStreams[$pathUri][$destinationPathUri] = $this->filesystem->open($outputPath, Mode::WRITE_READ);
        }

        return $this->writingStreams[$pathUri][$destinationPathUri];
    }
}
