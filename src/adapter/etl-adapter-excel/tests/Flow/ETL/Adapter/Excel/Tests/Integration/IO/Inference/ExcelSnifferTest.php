<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Integration\IO\Inference;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReadOptions;
use Flow\ETL\Adapter\Excel\IO\Inference\ExcelSniffer;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\IO\Inference\SampledUnits;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReversedSniffing;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;

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

final class ExcelSnifferTest extends FlowTestCase
{
    public function test_a_header_only_sheet_yields_string_columns(): void
    {
        static::assertEquals(
            schema(str_schema('id', nullable: true), str_schema('name', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('header_only.xlsx')),
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
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer(
                'nullable_fixture.xlsx',
                options: new ExcelReadOptions(convertEmptyToNull: false),
            )),
        );
    }

    public function test_all_files_are_sampled_when_unbounded(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSniffer('sniff/*', $filesystem),
            infer_schema()->everyRow()->everySource()->build(),
        );

        static::assertSame(2, $filesystem->readFromCalls);
    }

    public function test_a_bounded_sample_hands_its_open_sheet_over_once(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sniffer = ExcelFixtureContext::sniffer('sniff/a', $filesystem);
        ExcelFixtureContext::infer($sniffer, infer_schema()->sampleSize(1)->build());

        $sheet = $sniffer->take(ExcelFixtureContext::source('sniff/a'));

        static::assertNotNull($sheet);
        static::assertNull($sniffer->take(ExcelFixtureContext::source('sniff/a')));
        static::assertEquals(
            iterator_to_array(ExcelFixtureContext::sheet('sniff/a')->rows(), false),
            iterator_to_array($sheet->rows(), false),
        );
        // sniff/a is extension-less, so every open costs one readFrom: the sample's, and none for the rows
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_take_is_null_for_a_file_the_sample_never_opened(): void
    {
        static::assertNull(ExcelFixtureContext::sniffer('fixture.xlsx')->take(ExcelFixtureContext::source(
            'fixture.xlsx',
        )));
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
                ExcelFixtureContext::sniffer('int_float_bool.xlsx', inference: infer_schema()->allStrings()->build()),
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
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('nullable_fixture.xlsx')),
        );
    }

    public function test_an_empty_sheet_yields_an_empty_schema(): void
    {
        static::assertCount(
            0,
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('empty_sheet.xlsx'))->definitions(),
        );
    }

    public function test_dates_widen_inside_the_sample(): void
    {
        static::assertEquals(
            schema(date_schema('d', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('dates_only.xlsx')),
        );
        static::assertEquals(
            schema(datetime_schema('d', nullable: true)),
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('dates_mixed.xlsx')),
        );
    }

    public function test_files_to_sniff_stops_before_the_next_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSniffer('sniff/*', $filesystem),
            infer_schema()->everyRow()->filesToSniff(1)->build(),
        );

        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_header_comes_from_the_first_listed_sheet(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*', $filesystem);

        static::assertSame(['id', 'name', 'email'], $sniffer->header()->names);
        static::assertSame(['id', 'name', 'email'], $sniffer->header()->names);
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
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer(
                'fixture.xlsx',
                options: new ExcelReadOptions(withHeader: false),
            )),
        );
    }

    public function test_header_is_empty_when_nothing_is_listed(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sniffer = ExcelFixtureContext::globSniffer('sniff/nothing-here-*', $filesystem);

        static::assertSame([], $sniffer->header()->names);
        static::assertNull($sniffer->header()->source);
        static::assertSame(0, $sniffer->units());
        static::assertGreaterThan(0, $filesystem->listCalls);
        static::assertSame(0, $filesystem->readFromCalls);
    }

    public function test_header_skips_a_leading_workbook_that_resolves_no_names(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('empty_first/*.xlsx');

        static::assertSame(['id', 'name', 'email'], $sniffer->header()->names);
        static::assertSame(ExcelFixtureContext::path('empty_first/b.xlsx')->uri(), $sniffer->header()->source);
    }

    public function test_header_source_names_the_file_the_header_came_from(): void
    {
        static::assertSame(
            ExcelFixtureContext::path('sniff/a')->uri(),
            ExcelFixtureContext::globSniffer('sniff/*')->header()->source,
        );
    }

    public function test_infers_from_the_first_file(): void
    {
        $inferred = ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('fixture.xlsx'));

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
            ExcelFixtureContext::infer(ExcelFixtureContext::globSniffer('sniff/nothing-here-*'))->definitions(),
        );
    }

    public function test_sniff_folds_at_most_its_budget(): void
    {
        static::assertSame(3, ExcelFixtureContext::sniffer('sniff/a')->sniff(0, 3)->columns->rows);
        static::assertSame(1, ExcelFixtureContext::sniffer('sniff/a')->sniff(0, 1)->columns->rows);
        static::assertSame(10, ExcelFixtureContext::sniffer('sniff/a')->sniff(0, null)->columns->rows);
    }

    public function test_one_unit_per_sheet_and_a_sniff_reuses_the_sheet_the_header_came_from(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*', $filesystem);
        $units = $sniffer->units();
        $beforeAnyRead = $filesystem->readFromCalls;

        $names = $sniffer->names();
        $afterHeader = $filesystem->readFromCalls;

        // the first unit is the sheet names() read from, so its header row is already spent
        $firstSheet = $sniffer->sniff(0, null);

        static::assertSame(2, $units);
        static::assertSame(0, $beforeAnyRead);
        static::assertSame(['id', 'name', 'email'], $names);
        static::assertSame(1, $afterHeader);
        static::assertSame(10, $firstSheet->columns->rows);
        static::assertSame(1, $filesystem->readFromCalls);
    }

    public function test_sheet_name_reaches_the_sample(): void
    {
        static::assertEquals(
            schema(
                int_schema('id', nullable: true),
                str_schema('name', nullable: true),
                str_schema('email', nullable: true),
            ),
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer(
                'fixture.xlsx',
                options: new ExcelReadOptions(sheetName: 'Sheet2'),
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
                ExcelFixtureContext::sniffer('orders_flow.xlsx'),
                infer_schema()->sampleSize(50)->build(),
            ),
        );
    }

    public function test_the_row_budget_stops_before_the_next_file_is_opened(): void
    {
        $filesystem = new CountingFilesystem(native_local_filesystem());

        ExcelFixtureContext::infer(
            ExcelFixtureContext::globSniffer('sniff/*', $filesystem),
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
                ExcelFixtureContext::globSniffer('sniff/a', $filesystem),
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
            ExcelFixtureContext::infer(ExcelFixtureContext::sniffer('int_float_bool.xlsx')),
        );
    }

    public function test_a_file_no_sniff_reached_declares_unknown_rows(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*');

        static::assertEquals(
            Cardinality::unknown(),
            (new SampledUnits([$sniffer->sniff(0, 20_480)]))->exactRows($sniffer->units()),
        );
    }

    public function test_sniffs_that_read_every_sheet_declare_exact_rows(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*');

        static::assertEquals(
            Cardinality::exact(20),
            (new SampledUnits([$sniffer->sniff(0, 20_480), $sniffer->sniff(1, 20_480)]))->exactRows($sniffer->units()),
        );
    }

    public function test_an_unbounded_sample_counts_the_rows_it_streamed(): void
    {
        $sample = ExcelFixtureContext::sniffer('sniff/a')->sniff(0, null);

        static::assertSame([10, 0, true], [$sample->columns->rows, $sample->bytes, $sample->complete]);
    }

    public function test_a_unit_sniffer_sniffs_the_same_as_the_parent(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*');
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(ExcelSniffer::class, $unit);
        static::assertSame(1, $unit->units());
        static::assertSame($sniffer->names(), $unit->names());
        static::assertEquals($sniffer->sniff(1, null), $unit->sniff(0, null));

        $sniffer->close();
        $unit->close();
    }

    public function test_a_unit_sniffer_rebuilt_from_its_description_sniffs_the_same(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*');
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(ExcelSniffer::class, $unit);

        $description = $unit->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());

        $rebuilt = DescribeRoundTrip::rebuilt($unit);

        static::assertEquals($sniffer->sniff(1, null), $rebuilt->sniff(0, null));

        $sniffer->close();
        $rebuilt->close();
    }

    public function test_bytes_are_the_listed_size(): void
    {
        static::assertGreaterThan(0, ExcelFixtureContext::globSniffer('sniff/*')->bytes(0));
    }

    public function test_unbounded_inference_is_independent_of_execution_order(): void
    {
        $inference = (new SchemaInference(unionByName: true))->everyRow()->everySource();

        static::assertEquals(
            ExcelFixtureContext::infer(ExcelFixtureContext::globSniffer('sniff/*'), $inference),
            ReversedSniffing::schema(ExcelFixtureContext::globSniffer('sniff/*'), $inference),
        );
    }

    public function test_a_unit_sniffer_declares_its_workbook_readers_extensions(): void
    {
        $sniffer = ExcelFixtureContext::globSniffer('sniff/*');
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(ExcelSniffer::class, $unit);
        static::assertSame(['dom', 'filter', 'xmlreader', 'zip'], $unit->describe()?->extensions());

        $sniffer->close();
    }
}
