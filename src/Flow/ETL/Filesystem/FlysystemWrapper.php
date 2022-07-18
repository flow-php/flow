<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Stream\StreamWrapper;
use League\Flysystem\Filesystem;

abstract class FlysystemWrapper implements StreamWrapper
{
    /**
     * @var null|resource
     */
    public $context;

    protected ?LocalBuffer $buffer = null;

    protected ?Filesystem $filesystem = null;

    protected ?string $path = null;

    /**
     * @var null|resource
     */
    protected $stream;

    /**
     * @var null|array{path: ?string, host: string}
     */
    protected ?array $url = null;

    public function rename(string $path_from, string $path_to) : bool
    {
        throw new RuntimeException(__METHOD__ . ' not implemented');
    }

    public function rmdir(string $path, int $options) : bool
    {
        throw new RuntimeException(__METHOD__ . ' not implemented');
    }

    public function stream_close() : void
    {
        if (\is_resource($this->stream)) {
            /** @psalm-suppress InvalidPropertyAssignmentValue */
            \fclose($this->stream);
        }
    }

    public function stream_eof() : bool
    {
        $this->openRead();

        /**
         * @psalm-suppress PossiblyNullArgument
         * @phpstan-ignore-next-line
         */
        return \feof($this->stream);
    }

    public function stream_flush() : bool
    {
        $this->filesystem()->writeStream($this->path(), $this->buffer()->stream());

        $this->buffer()->release();

        return true;
    }

    public function stream_lock(int $operation) : bool
    {
        /**
         * @psalm-suppress PossiblyNullArgument
         * @phpstan-ignore-next-line
         */
        return \flock($this->stream, $operation);
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) : bool
    {
        $this->path = $path;
        /**
         * @psalm-suppress PropertyTypeCoercion
         * @phpstan-ignore-next-line
         */
        $this->url = \parse_url($this->path);

        $this->stream = match ($mode) {
            'r' => $this->filesystem()->readStream($this->path()),
            default => null
        };

        return true;
    }

    public function stream_read(int $count) : string|false
    {
        $this->openRead();

        /**
         * @psalm-suppress PossiblyNullArgument
         * @phpstan-ignore-next-line
         */
        return \fread($this->stream, $count);
    }

    public function stream_stat() : array|false
    {
        if (!$this->filesystem()->fileExists($this->path())) {
            return false;
        }

        return [
            'dev' => 0,
            'ino' => 0,
            'mode' => 0,
            'nlink' => 0,
            'uid' => 0,
            'gid' => 0,
            'rdev' => 0,
            'size' => $this->filesystem()->fileSize($this->path()),
            'atime' => 0,
            'ctime' => 0,
            'blksize' => 0,
            'blocks' => 0,
        ];
    }

    public function stream_write(string $data) : int
    {
        $this->buffer()->write($data);

        return \strlen($data);
    }

    public function url_stat(string $path, int $flags) : array|false
    {
        if (!$this->filesystem()->fileExists($path)) {
            return false;
        }

        return [
            'dev' => 0,
            'ino' => 0,
            'mode' => 0,
            'nlink' => 0,
            'uid' => 0,
            'gid' => 0,
            'rdev' => 0,
            'size' => $this->filesystem()->fileSize($path),
            'atime' => 0,
            'mtime' => $this->filesystem()->lastModified($path),
            'ctime' => 0,
            'blksize' => 0,
            'blocks' => 0,
        ];
    }

    abstract protected function filesystem() : Filesystem;

    protected function openRead() : void
    {
        if ($this->stream === null) {
            $this->stream = $this->filesystem()->readStream($this->path());
        }
    }

    private function buffer() : LocalBuffer
    {
        if ($this->buffer === null) {
            $this->buffer = new TmpfileBuffer();
        }

        return $this->buffer;
    }

    private function path() : string
    {
        if ($this->url === null) {
            return '';
        }

        return $this->url['path'] ?? $this->url['host'];
    }
}
