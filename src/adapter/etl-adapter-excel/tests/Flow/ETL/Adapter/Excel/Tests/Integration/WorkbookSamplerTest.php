<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration;

use Flow\ETL\Adapter\Excel\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function iterator_to_array;

final class WorkbookSamplerTest extends FlowTestCase
{
    public function test_a_header_only_sheet_yields_string_columns(): void
    {
        static::assertEquals(
            schema(str_schema('id', nullable: true), str_schema('name', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('header_only.xlsx')),
        );
    }

    public function test_a_kept_empty_cell_is_string_evidence(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler(
                'nullable_fixture.xlsx',
                options: new ExcelReadOptions(convertEmptyToNull: false),
            )),
        );
    }

    public function test_all_files_are_sampled_when_unbounded(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSampler('sniff/*', $filesystem),
            infer_schema()->sampleSize(-1)->filesToSniff(-1)->build(),
        );

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_all_strings_floors_every_column(): void
    {
        static::assertEquals(
            schema(
                str_schema('i', nullable: true),
                str_schema('f', nullable: true),
                str_schema('b', nullable: true),
                str_schema('s', nullable: true),
            ),
            ExcelFixtureContext::infer(
                ExcelFixtureContext::sampler('int_float_bool.xlsx'),
                infer_schema()->allStrings()->build(),
            ),
        );
    }

    public function test_an_empty_cell_is_no_evidence(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('nullable_fixture.xlsx')),
        );
    }

    public function test_an_empty_sheet_yields_an_empty_schema(): void
    {
        static::assertCount(
            0,
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('empty_sheet.xlsx'))->definitions(),
        );
    }

    public function test_dates_widen_inside_the_sample(): void
    {
        static::assertEquals(
            schema(date_schema('d', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('dates_only.xlsx')),
        );
        static::assertEquals(
            schema(datetime_schema('d', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('dates_mixed.xlsx')),
        );
    }

    public function test_files_to_sniff_stops_before_the_next_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSampler('sniff/*', $filesystem),
            infer_schema()->sampleSize(-1)->filesToSniff(1)->build(),
        );

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_header_comes_from_the_first_listed_sheet(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sampler = ExcelFixtureContext::globSampler('sniff/*', $filesystem);

        static::assertSame(['id', 'name', 'email'], $sampler->header()->names);
        static::assertSame(['id', 'name', 'email'], $sampler->header()->names);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_header_disabled_uses_generated_names(): void
    {
        static::assertEquals(
            schema(
                str_schema('e00', nullable: true),
                str_schema('e01', nullable: true),
                str_schema('e02', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler(
                'fixture.xlsx',
                options: new ExcelReadOptions(withHeader: false),
            )),
        );
    }

    public function test_header_is_empty_when_nothing_is_listed(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sampler = ExcelFixtureContext::globSampler('sniff/nothing-here-*', $filesystem);

        static::assertSame([], $sampler->header()->names);
        static::assertNull($sampler->header()->source);
        static::assertCount(0, iterator_to_array($sampler->samples(20_480), false));
        static::assertGreaterThan(0, $filesystem->listCalls);
        static::assertSame(0, $filesystem->readFromCalls);
    }

    public function test_header_skips_a_leading_workbook_that_resolves_no_names(): void
    {
        $sampler = ExcelFixtureContext::globSampler('empty_first/*.xlsx');

        static::assertSame(['id', 'name', 'email'], $sampler->header()->names);
        static::assertSame(ExcelFixtureContext::path('empty_first/b.xlsx')->uri(), $sampler->header()->source);
    }

    public function test_header_source_names_the_file_the_header_came_from(): void
    {
        static::assertSame(
            ExcelFixtureContext::path('sniff/a')->uri(),
            ExcelFixtureContext::globSampler('sniff/*')->header()->source,
        );
    }

    public function test_infers_from_the_first_file(): void
    {
        $inferred = ExcelFixtureContext::infer(ExcelFixtureContext::sampler('fixture.xlsx'));

        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            $inferred,
        );

        foreach ($inferred->definitions() as $definition) {
            static::assertTrue($definition->isNullable());
        }
    }

    public function test_no_files_yields_an_empty_schema(): void
    {
        static::assertCount(
            0,
            ExcelFixtureContext::infer(ExcelFixtureContext::globSampler('sniff/nothing-here-*'))->definitions(),
        );
    }

    public function test_samples_does_not_ration_the_row_budget(): void
    {
        foreach ([3, 1, -1] as $budget) {
            $samples = iterator_to_array(ExcelFixtureContext::sampler('sniff/a')->samples($budget), false);

            static::assertCount(10, iterator_to_array($samples[0], false));
        }
    }

    public function test_samples_yields_one_unstarted_generator_per_sheet(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sampler = ExcelFixtureContext::globSampler('sniff/*', $filesystem);
        $samples = iterator_to_array($sampler->samples(20_480), false);
        $beforeAnyAdvance = $filesystem->readFromCalls;

        $names = $sampler->header()->names;
        $afterHeader = $filesystem->readFromCalls;

        // the first inner generator is the sheet header() read from, so its header row is already spent
        $firstSheet = iterator_to_array($samples[0], false);

        static::assertCount(2, $samples);
        static::assertContainsOnlyInstancesOf(Generator::class, $samples);
        static::assertSame(0, $beforeAnyAdvance);
        static::assertSame(['id', 'name', 'email'], $names);
        static::assertSame(1, $afterHeader);
        static::assertCount(10, $firstSheet);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_sheet_name_and_offset_reach_the_sample(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler(
                'fixture.xlsx',
                options: new ExcelReadOptions(sheetName: 'Sheet2'),
            )),
        );

        static::assertEquals(
            schema(str_schema('v', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler(
                'int_then_str.xlsx',
                options: new ExcelReadOptions(offset: 4),
            )),
        );
    }

    public function test_text_narrows_only_to_types_a_cell_cannot_hold(): void
    {
        static::assertEquals(
            schema(
                uuid_schema('order_id', nullable: true),
                str_schema('created_at', nullable: true),
                str_schema('updated_at', nullable: true),
                str_schema('cancelled_at', nullable: true),
                str_schema('total_price', nullable: true),
                str_schema('discount', nullable: true),
                json_schema('customer', nullable: true),
                json_schema('address', nullable: true),
                json_schema('notes', nullable: true),
            ),
            ExcelFixtureContext::infer(
                ExcelFixtureContext::sampler('orders_flow.xlsx'),
                infer_schema()->sampleSize(50)->build(),
            ),
        );
    }

    public function test_the_row_budget_stops_before_the_next_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSampler('sniff/*', $filesystem),
            infer_schema()->sampleSize(10)->build(),
        );

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_the_row_budget_stops_inside_a_file(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            ExcelFixtureContext::infer(
                ExcelFixtureContext::globSampler('sniff/a', $filesystem),
                infer_schema()->sampleSize(3)->build(),
            ),
        );
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_typed_cells_keep_their_types(): void
    {
        static::assertEquals(
            schema(
                int_schema('i', nullable: true),
                float_schema('f', nullable: true),
                bool_schema('b', nullable: true),
                str_schema('s', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sampler('int_float_bool.xlsx')),
        );
    }
}
