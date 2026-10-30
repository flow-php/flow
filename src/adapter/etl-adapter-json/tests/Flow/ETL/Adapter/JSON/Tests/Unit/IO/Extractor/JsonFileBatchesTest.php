<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileBatches;
use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileReader;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
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
use function array_merge;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class JsonFileBatchesTest extends FlowTestCase
{
    public function test_a_document_yields_its_rows_consumes_no_offset_and_closes_its_stream(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with(['memory://a.json' => '[{"id":1},{"id":2},{"id":3}]']));
        $file = (new JsonFileBatches(
            $filesystem,
            new JsonFileReader($filesystem, JsonFraming::Array, null, false),
        ))->batches(
            FileReadMother::of(schema(int_schema('id')), null, 2),
            new Criteria(),
            new SourceFile(path('memory://a.json')),
            schema(int_schema('id')),
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array($file, false),
            )),
        );
        static::assertSame(0, $file->getReturn());
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_lines_yield_one_row_per_line(): void
    {
        $filesystem = MemoryFiles::with(['memory://a.jsonl' => "{\"id\":1}\n{\"id\":2}\n"]);

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new JsonFileBatches(
                        $filesystem,
                        new JsonFileReader($filesystem, JsonFraming::Lines, null, false),
                    ))->batches(
                        FileReadMother::of(schema(int_schema('id')), null, 10),
                        new Criteria(),
                        new SourceFile(path('memory://a.jsonl')),
                        schema(int_schema('id')),
                        new PhpBackend(),
                        new ReadWindow(),
                    ),
                    false,
                ),
            ),
        );
    }
}
