<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\Memory;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;

final class Memory
{
    /**
     * @var array<string, Path>
     */
    private array $paths = [];

    /**
     * @var array<string, MemoryStream>
     */
    private array $streams = [];

    public function __construct(private readonly ?\php_user_filter $filter = null)
    {

    }

    public function __destruct()
    {
        foreach ($this->streams as $stream) {
            $stream->close();
        }
    }

    public function close(Path $path) : void
    {
        if (\array_key_exists($path->uri(), $this->streams)) {
            unset($this->streams[$path->uri()], $this->paths[$path->uri()]);
        }
    }

    public function for(Path $path) : MemoryStream
    {
        if (\array_key_exists($path->uri(), $this->streams)) {
            return $this->streams[$path->uri()];
        }

        $outputStream = \mb_strtolower((string) $path->options()->getAsString('stream', 'temp'));

        if (!\in_array($outputStream, ['temp', 'memory'], true)) {
            throw new InvalidArgumentException('Invalid memory stream, allowed values are "temp" and "memory", given: ' . $outputStream);
        }

        $handle = fopen('php://' . $outputStream, 'wb');

        if ($this->filter !== null) {
            stream_filter_register($outputStream, $this->filter::class);
            /** @phpstan-ignore-next-line */
            stream_filter_append($handle, $this->filter::class);
            /** @phpstan-ignore-next-line */
            $this->streams[$path->uri()] = new MemoryStream($handle, $path);
        } else {
            /** @phpstan-ignore-next-line */
            $this->streams[$path->uri()] = new MemoryStream($handle, $path);
        }

        $this->paths[$path->uri()] = $path;

        return $this->streams[$path->uri()];
    }

    public function has(Path $path) : bool
    {
        return \array_key_exists($path->uri(), $this->streams);
    }

    /**
     * @return array<Path>
     */
    public function paths() : array
    {
        return \array_values($this->paths);
    }
}
