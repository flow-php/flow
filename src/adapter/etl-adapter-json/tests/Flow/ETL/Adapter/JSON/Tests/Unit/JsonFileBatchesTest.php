<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileBatches;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

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
        $source = new SourceFile(path('memory://a.json'));
        $file = (new JsonFileBatches(
            $filesystem,
            new JsonFileReader($filesystem, JsonFormat::Document, null, false, [$source]),
            JsonFormat::Document,
            null,
        ))->batches($source, schema(int_schema('id')), 2, new PhpBackend(), new ReadWindow());

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
        $source = new SourceFile(path('memory://a.jsonl'));
        $file = (new JsonFileBatches(
            $filesystem,
            new JsonFileReader($filesystem, JsonFormat::Lines, null, false, [$source]),
            JsonFormat::Lines,
            null,
        ))->batches($source, schema(int_schema('id')), 10, new PhpBackend(), new ReadWindow());

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
    }
}
