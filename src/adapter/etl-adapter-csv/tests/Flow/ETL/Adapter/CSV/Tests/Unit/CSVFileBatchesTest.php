<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVFileBatches;
use Flow\ETL\Adapter\CSV\CSVFileReader;
use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\InferredColumns;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class CSVFileBatchesTest extends FlowTestCase
{
    public function test_a_file_yields_its_rows_and_consumes_no_offset(): void
    {
        $filesystem = MemoryFiles::with(['memory://a.csv' => "id\n1\n2\n3\n"]);
        $source = new SourceFile(path('memory://a.csv'));
        $file = (new CSVFileBatches(new CSVFileReader($filesystem, new CSVReadOptions(), [$source]), null))->batches(
            $source,
            schema(int_schema('id')),
            2,
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
                new CSVFileReader($filesystem, new CSVReadOptions(), [$source]),
                new InferredColumns(schema(int_schema('id')), [], new SchemaInference(), 'memory://a.csv'),
            ))->batches($source, schema(int_schema('id')), 2, new PhpBackend(), new ReadWindow()));
            static::fail('a diverging header must be refused');
        } catch (InferredSchemaException $e) {
            static::assertStringContainsString('unexpected [age], missing [id]', $e->getMessage());
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }
}
