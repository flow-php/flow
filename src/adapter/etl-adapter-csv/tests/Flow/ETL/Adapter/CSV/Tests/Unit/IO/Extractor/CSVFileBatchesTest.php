<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVFileBatches;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function count;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class CSVFileBatchesTest extends FlowTestCase
{
    public function test_a_file_yields_its_rows_and_consumes_no_offset(): void
    {
        $file = (new CSVFileBatches(
            new CSVSourceOpener(MemoryFiles::with(['memory://a.csv' => "id\n1\n2\n3\n"]), new CSVReadOptions()),
            null,
        ))->batches(
            FileReadMother::of(schema(int_schema('id')), null, 2),
            new Criteria(),
            new SourceFile(path('memory://a.csv')),
            schema(int_schema('id')),
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['id' => 1], ['id' => 2]], [['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
        static::assertSame(0, $file->getReturn());
    }

    public function test_a_file_whose_header_diverges_is_refused_before_a_row_is_read(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with(['memory://b.csv' => "age\n1\n"]));
        $source = new SourceFile(path('memory://b.csv'));

        try {
            iterator_to_array((new CSVFileBatches(
                new CSVSourceOpener($filesystem, new CSVReadOptions()),
                new InferredColumns(schema(int_schema('id')), [], new SchemaInference(), 'memory://a.csv'),
            ))->batches(
                FileReadMother::of(schema(int_schema('id')), null, 2),
                new Criteria(),
                $source,
                schema(int_schema('id')),
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging header must be refused');
        } catch (InferredSchemaException $e) {
            static::assertStringContainsString('unexpected [age], missing [id]', $e->getMessage());
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }

    public function test_batches_are_sized_and_the_tail_is_shorter(): void
    {
        $sizes = [];

        foreach (CSVFixtureContext::fileBatches()->batches(
            FileReadMother::of(schema(int_schema('id'), str_schema('name')), null, 2),
            new Criteria(),
            CSVFixtureContext::source('five_rows.csv'),
            schema(int_schema('id'), str_schema('name')),
            new PhpBackend(),
            new ReadWindow(),
        ) as $batch) {
            $sizes[] = count($batch);
        }

        static::assertSame([2, 2, 1], $sizes);
    }

    public function test_batches_close_their_stream_when_abandoned(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $batches = CSVFixtureContext::fileBatches($counting)->batches(
            FileReadMother::of(schema(int_schema('id'), str_schema('name')), null, 2),
            new Criteria(),
            CSVFixtureContext::source('five_rows.csv'),
            schema(int_schema('id'), str_schema('name')),
            new PhpBackend(),
            new ReadWindow(),
        );

        $batches->current();
        unset($batches);

        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith(['empty.csv'])]
    #[TestWith(['header_only.csv'])]
    public function test_batches_never_yield_an_empty_batch(string $fixture): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                CSVFixtureContext::fileBatches()->batches(
                    FileReadMother::of(schema(int_schema('id'), str_schema('name')), null, 10),
                    new Criteria(),
                    CSVFixtureContext::source($fixture),
                    schema(int_schema('id'), str_schema('name')),
                    new PhpBackend(),
                    new ReadWindow(),
                ),
                false,
            ),
        );
    }

    public function test_batches_of_a_split_read_only_its_records(): void
    {
        $values = [];

        foreach (CSVFixtureContext::fileBatches()->batches(
            FileReadMother::of(schema(str_schema('id'), str_schema('name')), null, 10),
            new Criteria(split: new FileSplit(path(CSVFixtureContext::path('five_rows.csv')), 20, 8)),
            CSVFixtureContext::source('five_rows.csv'),
            schema(str_schema('id'), str_schema('name')),
            new PhpBackend(),
            new ReadWindow(),
        ) as $batch) {
            foreach ($batch->toArray() as $row) {
                $values[] = $row;
            }
        }

        static::assertSame([['id' => '4', 'name' => 'd'], ['id' => '5', 'name' => 'e']], $values);
    }
}
