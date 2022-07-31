<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Partition;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{path: Path, safe_mode: boolean}>
 */
final class JsonLoader implements Closure, Loader
{
    private ?FilesystemStreams $streams;

    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(
        private readonly Path $path,
        private bool $safeMode = false
    ) {
        $this->streams = null;
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'safe_mode' => $this->safeMode,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->safeMode = $data['safe_mode'];
        $this->streams = null;
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $streams = $this->streams;

        if ($streams !== null) {
            foreach ($streams as $stream) {
                $this->close($stream);
            }

            $streams->close();
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            foreach ($rows->partitionBy(...$context->partitionEntries()) as $partitionedRows) {
                $this->write($partitionedRows->rows, $partitionedRows->partitions, $context);
            }
        } else {
            $this->write($rows, [], $context);
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context) : void
    {
        $mode = Mode::WRITE;
        $streams = $this->streams($context);

        if ($context->mode() === SaveMode::ExceptionIfExists && $streams->exists($this->path, $partitions)) {
            throw new RuntimeException('Destination path "' . $this->path->uri() . '" already exists, please change path to different or set different SaveMode');
        }

        if ($context->mode() === SaveMode::Ignore && $streams->exists($this->path, $partitions) && !$streams->isOpen($this->path)) {
            return;
        }

        if ($context->mode() === SaveMode::Overwrite && $streams->exists($this->path, $partitions) && !$streams->isOpen($this->path, $partitions)) {
            $streams->rm($this->path, $partitions);
        }

        if ($context->mode() === SaveMode::Append && $streams->exists($this->path, $partitions)) {
            throw new RuntimeException('Append SaveMode is not yet supported in JSONLoader');
        }

        if (!$streams->isOpen($this->path, $partitions)) {
            $stream = $streams->open($this->path, 'json', $mode, $this->safeMode, $partitions);

            $this->init($stream);
        } else {
            $stream = $streams->open($this->path, 'json', $mode, $this->safeMode, $partitions);
        }

        $this->writeJSON($nextRows, $stream);
    }

    /**
     * @param Rows $rows
     * @param FileStream $stream
     *
     * @throws RuntimeException
     * @throws \JsonException
     */
    public function writeJSON(Rows $rows, FileStream $stream) : void
    {
        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($this->writes[$stream->path()->path()] > 0) ? ',' . $json : $json;

        \fwrite($stream->resource(), $json);

        $this->writes[$stream->path()->path()] += 1;
    }

    private function close(FileStream $stream) : void
    {
        \fwrite($stream->resource(), ']');
    }

    /**
     * @param FileStream $stream
     *
     * @throws RuntimeException
     */
    private function init(FileStream $stream) : void
    {
        if (!\array_key_exists($stream->path()->path(), $this->writes)) {
            $this->writes[$stream->path()->path()] = 0;
        }

        \fwrite($stream->resource(), '[');
    }

    private function streams(FlowContext $context) : FilesystemStreams
    {
        if ($this->streams === null) {
            $this->streams = new FilesystemStreams($context->fs());
        }

        return $this->streams;
    }
}
