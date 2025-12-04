<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

final class VoidStreamWrapper implements StreamWrapper
{
    public const PROTOCOL = 'void';

    /**
     * @var null|resource
     */
    public $context;

    #[\Override]
    public static function register() : void
    {
        if (!\in_array(self::PROTOCOL, \stream_get_wrappers(), true)) {
            \stream_wrapper_register(self::PROTOCOL, self::class);
        }
    }

    #[\Override]
    public function stream_close() : void
    {
    }

    #[\Override]
    public function stream_eof() : bool
    {
        return false;
    }

    #[\Override]
    public function stream_flush() : bool
    {
        return true;
    }

    #[\Override]
    public function stream_lock(int $operation) : bool
    {
        return true;
    }

    #[\Override]
    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) : bool
    {
        return true;
    }

    #[\Override]
    public function stream_read(int $count) : string|false
    {
        return false;
    }

    #[\Override]
    public function stream_seek(int $offset, int $whence = SEEK_SET) : bool
    {
        return false;
    }

    #[\Override]
    public function stream_stat() : array|false
    {
        return false;
    }

    #[\Override]
    public function stream_tell() : int
    {
        return 0;
    }

    #[\Override]
    public function stream_write(string $data) : int
    {
        return 0;
    }

    #[\Override]
    public function url_stat(string $path, int $flags) : array|false
    {
        return false;
    }
}
