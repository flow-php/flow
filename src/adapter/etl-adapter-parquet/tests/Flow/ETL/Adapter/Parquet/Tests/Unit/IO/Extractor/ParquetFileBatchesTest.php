<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetFileBatches;
use Flow\ETL\Adapter\Parquet\IO\Extractor\ParquetSourceFileOpener;
use Flow\ETL\Adapter\Parquet\Tests\Context\ParquetFilesContext;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\Writer\StatisticsLevel;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

use const NAN;

final class ParquetFileBatchesTest extends FlowTestCase
{
    public function test_an_offset_that_covers_the_file_skips_it_and_yields_its_row_count(): void
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

        static::assertEquals([new SkippedRows(2)], iterator_to_array($file, false));
    }

    public function test_an_offset_inside_the_file_starts_the_read_there_and_yields_the_rows_it_skipped(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::write($filesystem, ['memory://a.parquet' => array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id')))]);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
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
            ),
            false,
        );

        static::assertSame(
            [[['id' => 2], ['id' => 3]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), ParquetFilesContext::rows($batches)),
        );
        static::assertSame([1], ParquetFilesContext::skippedRows($batches));
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
            ParquetFilesContext::rows((new ParquetFileBatches(
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
            ))[0]->toArray(),
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

    public function test_a_conjunct_the_file_answers_exactly_is_left_out_of_the_yielded_filter(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $schema = schema(int_schema('id', nullable: true), str_schema('name', nullable: true));

        static::assertEquals(
            Filter::bind($schema, ref('name')->endsWith(lit('1'))),
            ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('id')->greaterThan(lit(5)), ref('name')->endsWith(lit('1'))]),
                new SourceFile(path('memory://groups.parquet')),
                $schema,
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_no_filter_is_yielded_when_the_file_answers_no_conjunct_exactly(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $schema = schema(int_schema('id', nullable: true), str_schema('name', nullable: true));

        static::assertNull(ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
            new PhpParquetEngine(),
            Options::default(),
            [],
            null,
        ))->batches(
            FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
            new Criteria(conjuncts: [ref('name')->endsWith(lit('1'))]),
            new SourceFile(path('memory://groups.parquet')),
            $schema,
            new PhpBackend(),
            new ReadWindow(0),
        )));
    }

    public function test_an_is_in_holding_null_stays_in_the_yielded_filter(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $schema = schema(int_schema('id', nullable: true), str_schema('name', nullable: true));

        static::assertEquals(
            Filter::bind($schema, not(ref('id')->isIn(lit([1, null])))),
            ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('id')->greaterThan(lit(5)), not(ref('id')->isIn(lit([1, null])))]),
                new SourceFile(path('memory://groups.parquet')),
                $schema,
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_a_greater_than_on_a_float_column_proven_free_of_nan_is_answered_exactly(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::scores($filesystem, 'memory://scores.parquet', [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        $schema = schema(int_schema('id', nullable: true), float_schema('score', nullable: true));

        static::assertEquals(
            Filter::bind($schema),
            ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('score')->greaterThan(lit(2.0))]),
                new SourceFile(path('memory://scores.parquet')),
                $schema,
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_a_greater_than_on_a_float_column_holding_nan_is_neither_pruned_nor_answered(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::scores($filesystem, 'memory://scores.parquet', [1.0, 2.0, NAN, 4.0, 5.0, 6.0]);
        $schema = schema(int_schema('id', nullable: true), float_schema('score', nullable: true));
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('score')->greaterThan(lit(100.0)), ref('id')->greaterThanEqual(lit(0))]),
                new SourceFile(path('memory://scores.parquet')),
                $schema,
                new PhpBackend(),
                new ReadWindow(0),
            ),
            false,
        );

        static::assertEquals(
            Filter::bind($schema, ref('score')->greaterThan(lit(100.0))),
            ParquetFilesContext::yieldedFilter($batches),
        );
        static::assertSame(6, ParquetFilesContext::rowCount($batches));
    }

    public function test_a_greater_than_on_a_float_column_without_statistics_stays_in_the_yielded_filter(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::scores(
            $filesystem,
            'memory://scores.parquet',
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            StatisticsLevel::NONE,
        );
        $schema = schema(int_schema('id', nullable: true), float_schema('score', nullable: true));

        static::assertEquals(
            Filter::bind($schema, ref('score')->greaterThan(lit(2.0))),
            ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('score')->greaterThan(lit(2.0)), ref('id')->greaterThanEqual(lit(0))]),
                new SourceFile(path('memory://scores.parquet')),
                $schema,
                new PhpBackend(),
                new ReadWindow(0),
            )),
        );
    }

    public function test_a_less_than_on_a_float_column_holding_nan_is_answered_exactly(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::scores($filesystem, 'memory://scores.parquet', [1.0, 2.0, NAN, 4.0, 5.0, 6.0]);
        $schema = schema(int_schema('id', nullable: true), float_schema('score', nullable: true));

        static::assertEquals(
            Filter::bind($schema),
            ParquetFilesContext::yieldedFilter((new ParquetFileBatches(
                new PhpParquetEngine(),
                Options::default(),
                [],
                null,
            ))->batches(
                FileReadMother::of($schema, FileColumnsContext::discovering(names: []), 10, $filesystem),
                new Criteria(conjuncts: [ref('score')->lessThan(lit(3.0))]),
                new SourceFile(path('memory://scores.parquet')),
                $schema,
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

    public function test_beside_an_exact_conjunct_a_file_whose_groups_are_all_pruned_skips_none_of_its_matches(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
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
                new ReadWindow(2, 3),
            ),
            false,
        );

        static::assertSame(0, ParquetFilesContext::rowCount($batches));
        static::assertSame([0], ParquetFilesContext::skippedRows($batches));
    }

    public function test_beside_exact_conjuncts_the_offset_and_the_limit_count_matching_rows(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
                FileReadMother::of(
                    schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('id')->greaterThan(lit(2))]),
                new SourceFile(path('memory://groups.parquet')),
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                new PhpBackend(),
                new ReadWindow(2, 3),
            ),
            false,
        );

        static::assertSame(
            [[['id' => 5, 'name' => 'n5'], ['id' => 6, 'name' => 'n6'], ['id' => 7, 'name' => 'n7']]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), ParquetFilesContext::rows($batches)),
        );
        static::assertSame([2], ParquetFilesContext::skippedRows($batches));
    }

    public function test_a_partition_conjunct_true_for_the_file_is_answered_so_the_file_takes_the_window(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://t/year=2024/groups.parquet', 9, 3);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
                FileReadMother::of(
                    schema(
                        int_schema('id', nullable: true),
                        str_schema('name', nullable: true),
                        str_schema('year', nullable: true),
                    ),
                    FileColumnsContext::discovering(names: ['year' => false]),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('year')->equals(lit('2024')), ref('id')->greaterThan(lit(2))]),
                new SourceFile(path('memory://t/year=2024/groups.parquet')),
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                new PhpBackend(),
                new ReadWindow(2, 3),
            ),
            false,
        );

        static::assertEquals(
            Filter::bind(schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('year', nullable: true),
            )),
            ParquetFilesContext::yieldedFilter($batches),
        );
        static::assertSame([2], ParquetFilesContext::skippedRows($batches));
        static::assertSame(3, ParquetFilesContext::rowCount($batches));
    }

    public function test_a_partition_conjunct_false_for_the_file_stays_in_the_yielded_filter(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://t/year=2024/groups.parquet', 9, 3);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
                FileReadMother::of(
                    schema(
                        int_schema('id', nullable: true),
                        str_schema('name', nullable: true),
                        str_schema('year', nullable: true),
                    ),
                    FileColumnsContext::discovering(names: ['year' => false]),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [$year = ref('year')->equals(lit('2023')), ref('id')->greaterThan(lit(2))]),
                new SourceFile(path('memory://t/year=2024/groups.parquet')),
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                new PhpBackend(),
                new ReadWindow(2, 3),
            ),
            false,
        );

        static::assertEquals(
            Filter::bind(
                schema(
                    int_schema('id', nullable: true),
                    str_schema('name', nullable: true),
                    str_schema('year', nullable: true),
                ),
                $year,
            ),
            ParquetFilesContext::yieldedFilter($batches),
        );
        static::assertSame([], ParquetFilesContext::skippedRows($batches));
        static::assertSame(6, ParquetFilesContext::rowCount($batches));
    }

    public function test_beside_a_residual_conjunct_the_offset_and_the_limit_are_left_to_the_loop(): void
    {
        $filesystem = memory_filesystem();
        ParquetFilesContext::rowGroups($filesystem, 'memory://groups.parquet', 9, 3);
        $batches = iterator_to_array(
            (new ParquetFileBatches(new PhpParquetEngine(), Options::default(), [], null))->batches(
                FileReadMother::of(
                    schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                    FileColumnsContext::discovering(names: []),
                    10,
                    $filesystem,
                ),
                new Criteria(conjuncts: [ref('id')->mod(2)->equals(lit(0))]),
                new SourceFile(path('memory://groups.parquet')),
                schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
                new PhpBackend(),
                new ReadWindow(2, 3),
            ),
            false,
        );

        static::assertSame(9, ParquetFilesContext::rowCount($batches));
        static::assertSame([], ParquetFilesContext::skippedRows($batches));
    }
}
