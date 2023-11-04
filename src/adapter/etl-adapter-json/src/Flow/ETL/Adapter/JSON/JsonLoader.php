<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Partition;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{path: Path}>
 */
final class JsonLoader implements Closure, Loader, Loader\FileLoader
{
    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(private readonly Path $path)
    {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("JsonLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
    }

    public function closure(FlowContext $context) : void
    {
        foreach ($context->streams() as $stream) {
            if ($stream->path()->extension() === 'json') {
                $this->close($stream);
            }
        }

        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($context->partitionEntries()->count()) {
            foreach ($rows->partitionBy(...$context->partitionEntries()->all()) as $partitionedRows) {
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
        $streams = $context->streams();

        if (!$streams->isOpen($this->path, $partitions)) {
            $stream = $streams->open($this->path, 'json', $mode, $context->threadSafe(), $partitions);

            $this->init($stream);
        } else {
            $stream = $streams->open($this->path, 'json', $mode, $context->threadSafe(), $partitions);
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
        if (!\count($rows)) {
            return;
        }

        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($this->writes[$stream->path()->path()] > 0) ? ',' . $json : $json;

        \fwrite($stream->resource(), $json);

        $this->writes[$stream->path()->path()]++;
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
}
