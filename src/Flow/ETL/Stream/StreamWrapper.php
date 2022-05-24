<?php declare(strict_types=1);

namespace Flow\ETL\Stream;

/**
 * @property resource $context
 */
interface StreamWrapper
{
    public const PROTOCOL = 'file://';

    public static function register() : void;

    public function dir_closedir() : bool;

    public function dir_opendir(string $path, int $options) : bool;

    public function dir_readdir() : string;

    public function dir_rewinddir() : bool;

    public function mkdir(string $path, int $mode, int $options) : bool;

    public function rename(string $path_from, string $path_to) : bool;

    public function rmdir(string $path, int $options) : bool;

    /**
     * @param int $cast_as
     *
     * @return resource
     */
    public function stream_cast(int $cast_as);

    public function stream_close() : void;

    public function stream_eof() : bool;

    public function stream_flush() : bool;

    public function stream_lock(int $operation) : bool;

    public function stream_metadata(string $path, int $option, mixed $value) : bool;

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) : bool;

    public function stream_read(int $count) : string|false;

    public function stream_seek(int $offset, int $whence = SEEK_SET) : bool;

    public function stream_set_option(int $option, int $arg1, int $arg2) : bool;

    /**
     * @return array<mixed>|false
     */
    public function stream_stat() : array|false;

    public function stream_tell() : int;

    public function stream_truncate(int $new_size) : bool;

    public function stream_write(string $data) : int;

    public function unlink(string $path) : bool;

    /**
     * @param string $path
     * @param int $flags
     *
     * @return array<mixed>|false
     */
    public function url_stat(string $path, int $flags) : array|false;
}
