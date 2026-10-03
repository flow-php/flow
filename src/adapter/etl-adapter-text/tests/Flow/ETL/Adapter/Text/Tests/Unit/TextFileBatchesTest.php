<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit;

use Flow\ETL\Adapter\Text\TextFileBatches;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class TextFileBatchesTest extends FlowTestCase
{
    public function test_lines_land_in_the_text_column_and_no_offset_is_consumed(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with(['memory://a.txt' => "a\nb\nc\n"]));
        $file = (new TextFileBatches($filesystem))->batches(
            new SourceFile(path('memory://a.txt')),
            schema(str_schema('text')),
            2,
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['text' => 'a'], ['text' => 'b']], [['text' => 'c']]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
        static::assertSame(0, $file->getReturn());
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_a_column_the_body_declares_beside_text_is_padded_with_nulls(): void
    {
        $file = (new TextFileBatches(MemoryFiles::with(['memory://a.txt' => "a\n"])))->batches(
            new SourceFile(path('memory://a.txt')),
            schema(str_schema('text'), str_schema('note', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['text' => 'a', 'note' => null]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
    }
}
