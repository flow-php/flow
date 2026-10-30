<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Text\IO\Extractor\TextFileBatches;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

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
        $file = (new TextFileBatches())->batches(
            FileReadMother::of(schema(str_schema('text')), null, 2, $filesystem),
            new Criteria(),
            new SourceFile(path('memory://a.txt')),
            schema(str_schema('text')),
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
        static::assertSame(
            [[['text' => 'a', 'note' => null]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new TextFileBatches())->batches(
                        FileReadMother::of(
                            schema(str_schema('text'), str_schema('note', nullable: true)),
                            null,
                            10,
                            MemoryFiles::with(['memory://a.txt' => "a\n"]),
                        ),
                        new Criteria(),
                        new SourceFile(path('memory://a.txt')),
                        schema(str_schema('text'), str_schema('note', nullable: true)),
                        new PhpBackend(),
                        new ReadWindow(),
                    ),
                    false,
                ),
            ),
        );
    }
}
