<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\Text\IO\Loader\TextEncoder;
use Flow\ETL\Adapter\Text\IO\Loader\TextFileWriter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class TextFileWriterTest extends FlowTestCase
{
    public function test_every_write_appends_its_lines_and_close_adds_nothing(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://out.txt'));
        $sink = new TextFileWriter($stream, new TextEncoder("\n"));

        $sink->write(array_to_rows([['text' => 'a'], ['text' => 'b']], schema(str_schema('text'))));
        $sink->write(array_to_rows([['text' => 'c']], schema(str_schema('text'))));
        $sink->close();
        $stream->close();

        static::assertSame("a\nb\nc\n", $filesystem->readFrom(path('memory://out.txt'))->content());
    }
}
