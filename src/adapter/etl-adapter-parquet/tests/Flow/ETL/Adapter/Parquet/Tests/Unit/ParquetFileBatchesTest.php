<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\ParquetFileBatches;
use Flow\ETL\Adapter\Parquet\ParquetSourceFileOpener;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ParquetFileBatchesTest extends FlowTestCase
{
    public function test_an_offset_that_covers_the_file_skips_it_and_returns_its_rows(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::write($filesystem, ['memory://a.parquet' => array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id')))]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
        $file = (new ParquetFileBatches($opener, FileColumnsContext::discovering(names: []), null, null))->batches(
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(2),
        );

        static::assertSame([], iterator_to_array($file, false));
        static::assertSame(2, $file->getReturn());
    }

    public function test_an_offset_inside_the_file_starts_the_read_there_and_is_consumed(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::write($filesystem, ['memory://a.parquet' => array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
        $file = (new ParquetFileBatches($opener, FileColumnsContext::discovering(names: []), null, null))->batches(
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(1),
        );

        static::assertSame(
            [[['id' => 2], ['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
        static::assertSame(1, $file->getReturn());
    }

    public function test_the_kept_first_file_is_read_instead_of_opened_again(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        ParquetFilesContext::write($filesystem, ['memory://a.parquet' => array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
        $kept = $opener->open(new SourceFile(path('memory://a.parquet')));
        $opened = $filesystem->readFromCalls;

        iterator_to_array((new ParquetFileBatches(
            $opener,
            FileColumnsContext::discovering(names: []),
            $kept,
            null,
        ))->batches(
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(),
        ));

        static::assertSame($opened, $filesystem->readFromCalls);
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_a_kept_first_file_of_another_path_is_closed_and_the_file_opened(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        ParquetFilesContext::write($filesystem, [
            'memory://a.parquet' => array_to_rows([['id' => 1]], schema(int_schema('id'))),
            'memory://b.parquet' => array_to_rows([['id' => 2]], schema(int_schema('id'))),
        ]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
        $kept = $opener->open(new SourceFile(path('memory://a.parquet')));

        $rows = iterator_to_array(
            (new ParquetFileBatches($opener, FileColumnsContext::discovering(names: []), $kept, null))->batches(
                new SourceFile(path('memory://b.parquet')),
                schema(int_schema('id', nullable: true)),
                10,
                new PhpBackend(),
                new ReadWindow(),
            ),
            false,
        );

        static::assertSame([['id' => 2]], $rows[0]->toArray());
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_close_releases_a_kept_first_file_the_read_never_reached(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        ParquetFilesContext::write($filesystem, ['memory://a.parquet' => array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );
        $kept = $opener->open(new SourceFile(path('memory://a.parquet')));

        (new ParquetFileBatches($opener, FileColumnsContext::discovering(names: []), $kept, null))->close();

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_a_file_that_diverges_from_the_derived_schema_is_refused_and_closed(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        ParquetFilesContext::write($filesystem, ['memory://b.parquet' => array_to_rows([[
            'name' => 'x',
        ]], schema(str_schema('name')))]);
        $opener = new ParquetSourceFileOpener(
            $filesystem,
            new PhpParquetEngine(),
            Options::default(),
            new SchemaConverter(),
            [],
        );

        try {
            iterator_to_array((new ParquetFileBatches(
                $opener,
                FileColumnsContext::discovering(names: []),
                null,
                new DerivedSchema(schema(int_schema('id', nullable: true)), 'memory://a.parquet'),
            ))->batches(
                new SourceFile(path('memory://b.parquet')),
                schema(int_schema('id')),
                10,
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging file must be refused');
        } catch (InferredSchemaException) {
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }
}
