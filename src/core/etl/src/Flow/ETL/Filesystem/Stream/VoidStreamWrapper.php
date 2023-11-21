<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

final class VoidStreamWrapper implements StreamWrapper
{
    public const PROTOCOL = 'void';

    public static function register() : void
    {
        if (!\in_array(self::PROTOCOL, \stream_get_wrappers(), true)) {
            \stream_wrapper_register(self::PROTOCOL, self::class);
        }
    }

    public function stream_close() : void
    {
    }

    public function stream_eof() : bool
    {
        return false;
    }

    public function stream_flush() : bool
    {
        return true;
    }

    public function stream_lock(int $operation) : bool
    {
        return true;
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) : bool
    {
        return true;
    }

    public function stream_read(int $count) : string|false
    {
        return false;
    }

    public function stream_stat() : array|false
    {
        return false;
    }

    public function stream_write(string $data) : int
    {
        return 0;
    }

    public function url_stat(string $path, int $flags) : array|false
    {
        return false;
    }
}
