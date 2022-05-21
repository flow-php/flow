<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Rows;
use Ramsey\Uuid\Uuid;

/**
 * @implements Loader<array{path: string, safe_mode: boolean}>
 */
final class JsonLoader implements Closure, Loader
{
    /**
     * @var null|resource
     */
    private $stream;

    public function __construct(private string $path, private bool $safeMode = false)
    {
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
    public function load(Rows $rows) : void
    {
        /** @var array{size:int} $stats */
        $stats = \fstat($this->stream());

        $json = \substr(\substr(\json_encode($rows->toArray(), JSON_THROW_ON_ERROR), 0, -1), 1);
        $json = ($stats['size'] > 2)
            ? ',' . $json
            : $json;

        \fwrite($this->stream(), $json);
    }

    public function closure(Rows $rows) : void
    {
        $stream = $this->stream();

        \fwrite($stream, ']');
        \fclose($stream);
    }

    /**
     * @return resource
     * @psalm-suppress InvalidNullableReturnType
     */
    private function stream()
    {
        if ($this->stream === null) {
            $fullPath = ($this->safeMode)
                ? (\rtrim($this->path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . Uuid::uuid4()->toString() . '.csv')
                : $this->path;

            if ($this->safeMode) {
                \mkdir(\rtrim($this->path, DIRECTORY_SEPARATOR));
            }

            $stream = \fopen($fullPath, 'w+');

            if ($stream === false) {
                throw new RuntimeException("Unable to open stream for path {$this->path}.");
            }

            $this->stream = $stream;
            \fwrite($this->stream, '[');
        }

        return $this->stream;
    }
}
