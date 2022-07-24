<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

/**
 * @property resource $context
 */
interface StreamWrapper
{
    public const PROTOCOL = 'file://';

    public static function register() : void;

    public function stream_close() : void;

    public function stream_eof() : bool;

    public function stream_flush() : bool;

    public function stream_lock(int $operation) : bool;

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) : bool;

    public function stream_read(int $count) : string|false;

    /**
     * @return array<mixed>|false
     */
    public function stream_stat() : array|false;

    public function stream_write(string $data) : int;

    /**
     * @param string $path
     * @param int $flags
     *
     * @return array<mixed>|false
     */
    public function url_stat(string $path, int $flags) : array|false;
}
