<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetFileBatches;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetSourceFileOpener;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
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
        $file = (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
            FileReadMother::of(
                schema(int_schema('id', nullable: true)),
                FileColumnsContext::discovering(names: []),
                10,
                $filesystem,
            ),
            new Criteria(),
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
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
        $file = (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
            FileReadMother::of(
                schema(int_schema('id', nullable: true)),
                FileColumnsContext::discovering(names: []),
                10,
                $filesystem,
            ),
            new Criteria(),
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
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
            new PhpParquetEngine(),
            Options::default(),
            [],
            null,
            $kept,
        ))->batches(
            FileReadMother::of(
                schema(int_schema('id', nullable: true)),
                FileColumnsContext::discovering(names: []),
                10,
                $filesystem,
            ),
            new Criteria(),
            new SourceFile(path('memory://a.parquet')),
            schema(int_schema('id', nullable: true)),
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

        static::assertSame(
            [['id' => 2]],
            iterator_to_array(
                (new ParquetFileBatches(
                    new PhpParquetEngine(),
                    Options::default(),
                    [],
                    null,
                    $opener->open(new SourceFile(path('memory://a.parquet'))),
                ))->batches(
                    FileReadMother::of(
                        schema(int_schema('id', nullable: true)),
                        FileColumnsContext::discovering(names: []),
                        10,
                        $filesystem,
                    ),
                    new Criteria(),
                    new SourceFile(path('memory://b.parquet')),
                    schema(int_schema('id', nullable: true)),
                    new PhpBackend(),
                    new ReadWindow(),
                ),
                false,
            )[0]->toArray(),
        );
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

        (new ParquetFileBatches(
            new PhpParquetEngine(),
            Options::default(),
            [],
            null,
            $opener->open(new SourceFile(path('memory://a.parquet'))),
        ))->close();

        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_a_file_that_diverges_from_the_derived_schema_is_refused_and_closed(): void
    {
        $filesystem = new CountingFilesystem(memory_filesystem());
        ParquetFilesContext::write($filesystem, ['memory://b.parquet' => array_to_rows([[
            'name' => 'x',
        ]], schema(str_schema('name')))]);

        try {
            iterator_to_array((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                new DerivedSchema(schema(int_schema('id', nullable: true)), 'memory://a.parquet'),
            ))->batches(
                FileReadMother::of(
                    schema(int_schema('id')),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(),
                new SourceFile(path('memory://b.parquet')),
                schema(int_schema('id')),
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging file must be refused');
        } catch (InferredSchemaException) {
            static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
        }
    }

    public function test_a_body_predicate_skips_the_row_groups_it_proves_cannot_match(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);

        static::assertSame(
            3,
            ParquetFilesContext::rowCount((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of(
                    schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('id')->greaterThan(lit(5))]),
                new SourceFile(path('memory://groups.parquet')),
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_the_conjunct_on_a_column_the_file_stores_as_another_type_is_dropped(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::unsignedInt64($filesystem, 'memory://uint64.parquet', ['9', '10', '100']);
        // the body reads the UINT_64 leaf as a string, ordered by bytes: '10' and '100' are below '2', numerically none is
        $file = (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
            FileReadMother::of(
                schema(str_schema('big', nullable: true)),
                FileColumnsContext::discovering(names: []),
                10,
                $filesystem,
            ),
            new Criteria(conjuncts: [ref('big')->lessThan(lit('2'))]),
            new SourceFile(path('memory://uint64.parquet')),
            schema(str_schema('big', nullable: true)),
            new PhpBackend(),
            new ReadWindow(0),
        );

        static::assertSame(3, ParquetFilesContext::rowCount($file));
    }

    public function test_equality_on_a_column_absent_from_the_file_reads_no_row(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);

        static::assertSame(
            0,
            ParquetFilesContext::rowCount((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of(
                    schema(
                        int_schema('id', nullable: true),
                        str_schema('name', nullable: true),
                        str_schema('missing', nullable: true),
                    ),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('missing')->equals(lit('x'))]),
                new SourceFile(path('memory://groups.parquet')),
                schema(
                    int_schema('id', nullable: true),
                    str_schema('name', nullable: true),
                    str_schema('missing', nullable: true),
                ),
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_is_null_on_a_column_absent_from_the_file_reads_every_row(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);

        static::assertSame(
            9,
            ParquetFilesContext::rowCount((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of(
                    schema(
                        int_schema('id', nullable: true),
                        str_schema('name', nullable: true),
                        str_schema('missing', nullable: true),
                    ),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('missing')->isNull()]),
                new SourceFile(path('memory://groups.parquet')),
                schema(
                    int_schema('id', nullable: true),
                    str_schema('name', nullable: true),
                    str_schema('missing', nullable: true),
                ),
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_every_group_pruned_still_consumes_the_raw_offset(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $file = (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
            FileReadMother::of(
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                FileColumnsContext::discovering(names: []),
                10,
                $filesystem,
            ),
            new Criteria(conjuncts: [ref('id')->greaterThan(lit(100))]),
            new SourceFile(path('memory://groups.parquet')),
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            new PhpBackend(),
            new ReadWindow(2),
        );

        static::assertSame(0, ParquetFilesContext::rowCount($file));
        static::assertSame(2, $file->getReturn());
    }
}
