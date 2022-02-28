<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Formatter;
use Flow\ETL\Loader;
use Flow\ETL\Memory\Memory;

class To
{
    final public static function buffer(Loader $overflowLoader, int $bufferSize) : Loader
    {
        return new Loader\BufferLoader($overflowLoader, $bufferSize);
    }

    final public static function callback(callable $callable) : Loader
    {
        return new Loader\CallbackLoader($callable);
    }

    final public static function memory(Memory $memory) : Loader
    {
        return new Loader\MemoryLoader($memory);
    }

    final public static function output(int $truncate = 20, Formatter $formatter = null) : Loader
    {
        return Loader\StreamLoader::output($truncate, $formatter);
    }

    final public static function stderr(int $truncate = 20, Formatter $formatter = null) : Loader
    {
        return Loader\StreamLoader::stderr($truncate, $formatter);
    }

    final public static function stdout(int $truncate = 20, Formatter $formatter = null) : Loader
    {
        return Loader\StreamLoader::stdout($truncate, $formatter);
    }

    final public static function stream(string $uri, string $mode = 'w', int $truncate = 20, Formatter $formatter = null) : Loader
    {
        return new Loader\StreamLoader($uri, $mode, $truncate, $formatter);
    }
}
