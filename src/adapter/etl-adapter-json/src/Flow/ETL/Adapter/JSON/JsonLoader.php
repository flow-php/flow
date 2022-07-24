<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
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

    /**
     * @param Rows $rows
     * @param FileStream $stream
     *
     * @throws RuntimeException
     * @throws \JsonException
     */
    public function append(Rows $rows, FileStream $stream) : void
    {
        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($this->writes[$stream->path()->path()] > 0) ? ',' . $json : $json;

        \fwrite($stream->resource(), $json);

        $this->writes[$stream->path()->path()] += 1;
    }

    public function close(FileStream $stream) : void
    {
        \fwrite($stream->resource(), ']');
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

    /**
     * @param FileStream $stream
     *
     * @throws RuntimeException
     */
    public function init(FileStream $stream) : void
    {
        if (!\array_key_exists($stream->path()->path(), $this->writes)) {
            $this->writes[$stream->path()->path()] = 0;
        }

        \fwrite($stream->resource(), '[');
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($this->streams === null) {
            $this->streams = new FilesystemStreams($context->fs());
        }

        $streams = $this->streams;

        if (\count($context->partitionEntries())) {
            foreach ($rows->partitionBy(...$context->partitionEntries()) as $partitionedRows) {
                if (!$streams->exists($this->path, $partitionedRows->partitions)) {
                    $stream = $streams->open($this->path, 'json', Mode::WRITE, $this->safeMode);

                    $this->init($stream);
                } else {
                    $stream = $streams->open($this->path, 'json', Mode::WRITE, $this->safeMode, $partitionedRows->partitions);
                }

                $this->append($partitionedRows->rows, $stream);
            }
        } else {
            if (!$streams->exists($this->path)) {
                $stream = $streams->open($this->path, 'json', Mode::WRITE, $this->safeMode);

                $this->init($stream);
            } else {
                $stream = $streams->open($this->path, 'json', Mode::WRITE, $this->safeMode);
            }

            $this->append($rows, $stream);
        }
    }
}
