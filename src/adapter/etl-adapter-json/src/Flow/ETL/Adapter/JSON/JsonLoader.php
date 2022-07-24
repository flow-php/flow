<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
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
    private ?FileStream $fileStream;

    private int $writes = 0;

    public function __construct(
        private readonly Path $path,
        private bool $safeMode = false
    ) {
        $this->fileStream = null;
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
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            throw new RuntimeException('Partitioning is not supported yet');
        }

        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($this->writes > 0) ? ',' . $json : $json;

        \fwrite($this->stream($context)->resource(), $json);

        $this->writes += 1;
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $stream = $this->stream($context);

        \fwrite($stream->resource(), ']');
        $stream->close();
        $this->fileStream = null;
    }

    /**
     * @throws RuntimeException
     *
     * @psalm-suppress InvalidNullableReturnType
     */
    private function stream(FlowContext $context) : FileStream
    {
        if ($this->fileStream === null) {
            $this->fileStream = $context->fs()->open(
                $this->safeMode ? $this->path->randomize() : $this->path,
                Mode::WRITE
            );

            \fwrite($this->fileStream->resource(), '[');
        }

        return $this->fileStream;
    }
}
