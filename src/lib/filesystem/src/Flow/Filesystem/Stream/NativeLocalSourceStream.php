<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\{Exception\InvalidArgumentException, Exception\RuntimeException, Path, SourceStream};

final class NativeLocalSourceStream implements SourceStream
{
    /**
     * @var null|closed-resource|resource
     */
    private $handle;

    /**
     * @param Path $path
     * @param resource $handle
     */
    public function __construct(private readonly Path $path, $handle)
    {
        if (!\is_resource($handle)) {
            throw new InvalidArgumentException('SourceStream expects resource type, given: ' . \gettype($handle));
        }

        $this->handle = $handle;
    }

    public static function open(Path $path) : self
    {
        $resource = \fopen($path->path(), 'rb', false, $path->context()->resource());

        if ($resource === false) {
            throw new RuntimeException("Cannot open file: {$path->uri()}");
        }

        return new self($path, $resource);
    }

    public function close() : void
    {
        if (!\is_resource($this->handle)) {
            $this->handle = null;

            return;
        }

        \fclose($this->handle());
        $this->handle = null;
    }

    public function content() : string
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        \fseek($this->handle(), 0);

        $content = \stream_get_contents($this->handle());

        if ($content === false) {
            throw new RuntimeException("Cannot read file content: {$this->path->uri()}");
        }

        return $content;
    }

    public function isOpen() : bool
    {
        return \is_resource($this->handle);
    }

    /**
     * @param int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function iterate(int $length = 1) : \Generator
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        \fseek($this->handle(), 0);

        while (!\feof($this->handle())) {
            $string = \fread($this->handle(), $length);

            if ($string === false) {
                break;
            }

            yield $string;
        }
    }

    public function path() : Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset) : string
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        \fseek($this->handle(), $offset, $offset < 0 ? \SEEK_END : \SEEK_SET);

        $result = \fread($this->handle(), $length);

        return $result === false ? '' : $result;
    }

    /**
     * @param ?int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        \fseek($this->handle(), 0);

        while (!\feof($this->handle())) {
            $line = \stream_get_line($this->handle(), \PHP_INT_MAX, $separator);

            if ($line === false) {
                break;
            }

            yield $line;
        }
    }

    public function size() : int
    {
        $size = \filesize($this->path->path());

        if ($size === false) {
            throw new RuntimeException("Cannot get file size: {$this->path->uri()}");
        }

        return $size;
    }

    /**
     * @return resource
     */
    private function handle()
    {
        if (!$this->isOpen() || $this->handle === null) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        return $this->handle;
    }
}
