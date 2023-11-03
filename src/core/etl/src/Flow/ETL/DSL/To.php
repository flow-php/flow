<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Formatter;
use Flow\ETL\Loader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\SchemaFormatter;
use Flow\ETL\Transformer;

/**
 * @infection-ignore-all
 */
class To
{
    final public static function callback(callable $callable) : Loader
    {
        return new Loader\CallbackLoader($callable);
    }

    final public static function memory(Memory $memory) : Loader
    {
        return new Loader\MemoryLoader($memory);
    }

    final public static function output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : Loader
    {
        return Loader\StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
    }

    final public static function stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : Loader
    {
        return Loader\StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
    }

    final public static function stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : Loader
    {
        return Loader\StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
    }

    final public static function stream(string $uri, int|bool $truncate = 20, Output $output = Output::rows, string $mode = 'w', Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : Loader
    {
        return new Loader\StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter);
    }

    final public static function transform_to(Transformer $transformer, Loader $loader) : Loader
    {
        return new Loader\TransformerLoader($transformer, $loader);
    }
}
