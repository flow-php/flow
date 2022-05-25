<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\LocalFile;
use Flow\ETL\Stream\Mode;

/**
 * @implements Loader<array{stream: FileStream, safe_mode: boolean}>
 */
final class JsonLoader implements Closure, Loader
{
    /**
     * @var null|resource
     */
    private $resource;

    private Handler $handler;

    private int $writes = 0;

    private FileStream $stream;

    public function __construct(
        FileStream|string $stream,
        private bool $safeMode = false
    ) {
        if (\is_string($stream)) {
            $this->stream = new LocalFile($stream);
        } else {
            $this->stream = $stream;
        }

        $this->handler = $this->safeMode ? Handler::directory('json') : Handler::file();
    }

    public function __serialize() : array
    {
        return [
            'stream' => $this->stream,
            'safe_mode' => $this->safeMode,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->stream = $data['stream'];
        $this->safeMode = $data['safe_mode'];
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    public function load(Rows $rows) : void
    {
        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($this->writes > 0) ? ',' . $json : $json;

        \fwrite($this->stream(), $json);

        $this->writes += 1;
    }

    public function closure(Rows $rows) : void
    {
        $stream = $this->stream();

        \fwrite($stream, ']');
        \fclose($stream);
    }

    /**
     * @throws RuntimeException
     *
     * @return resource
     * @psalm-suppress InvalidNullableReturnType
     */
    private function stream()
    {
        if ($this->resource === null) {
            $this->resource = $this->handler->open($this->stream, Mode::WRITE);

            \fwrite($this->resource, '[');
        }

        return $this->resource;
    }
}
