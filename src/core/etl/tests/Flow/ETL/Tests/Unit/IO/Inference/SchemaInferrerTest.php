<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Tests\Context\ReversedSniffing;
use Flow\ETL\Tests\Double\FakeSniffer;
use Flow\ETL\Tests\Double\PresniffedSniffer;
use Flow\ETL\Tests\Double\RecordingLauncher;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;
use Flow\ETL\Values\Schema;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function array_slice;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class SchemaInferrerTest extends FlowTestCase
{
    /**
     * The budget decides how much of the heterogeneous fixture is seen, so each row is a different schema.
     *
     * @return array<string, array{null|int<1, max>, array<string, string>, array<int, int>}>
     */
    public static function budgets(): array
    {
        return [
            'inside the first source' => [5, ['a' => 'integer', 'b' => 'string'], [0 => 5]],
            'exactly on the first boundary' => [10, ['a' => 'integer', 'b' => 'string'], [0 => 10]],
            'into the source that widens a' => [15, ['a' => 'float', 'b' => 'string'], [0 => 10, 1 => 5]],
            'into the source that adds c' => [
                25,
                ['a' => 'float', 'b' => 'string', 'c' => 'boolean'],
                [0 => 10, 1 => 10, 2 => 5],
            ],
            'unbounded' => [null, ['a' => 'float', 'b' => 'string', 'c' => 'boolean'], [0 => 10, 1 => 10, 2 => 10]],
        ];
    }

    /**
     * Heterogeneous on purpose: "a" widens integer -> float inside source 1, and source 2 introduces "c", so a
     * budget that stops early and a merge that drops a name are both observable.
     *
     * @return list<list<array<array-key, mixed>>>
     */
    public static function threeSourcesOfTenRows(): array
    {
        $sources = [];

        for ($source = 0; $source < 3; $source++) {
            $rows = [];

            for ($row = 0; $row < 10; $row++) {
                $values = ['a' => (string) $row, 'b' => 'x'];

                if ($source === 1 && $row === 2) {
                    $values['a'] = '1.5';
                }

                if ($source === 2) {
                    $values['c'] = 'true';
                }

                $rows[] = $values;
            }

            $sources[] = $rows;
        }

        return $sources;
    }

    public function test_every_unit_is_asked_with_the_budget_left(): void
    {
        $sniffer = new FakeSniffer(
            [
                [
                    ['a' => '1'],
                    ['a' => '2'],
                    ['a' => '3'],
                    ['a' => '4'],
                ],
                [
                    ['a' => '1.5', 'b' => 'x'],
                    ['a' => '2.5', 'b' => 'y'],
                    ['a' => '3.5', 'b' => 'z'],
                ],
            ],
            new StringTypeNarrower(),
            ['a'],
        );

        $schema = (new SchemaInferrer(new SchemaInference(sampleSize: 10, unionByName: true)))->infer($sniffer)->schema;

        static::assertSame([[0, 10], [1, 6]], $sniffer->sniffed);
        static::assertEquals(
            new Schema(
                definition_from_type('a', type_float(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $schema,
        );
    }

    public function test_a_unit_is_not_asked_once_the_budget_is_spent(): void
    {
        $tenIntegers = [];

        for ($row = 0; $row < 10; $row++) {
            $tenIntegers[] = ['a' => (string) $row];
        }

        $sniffer = new FakeSniffer([$tenIntegers, $tenIntegers], new StringTypeNarrower(), ['a']);

        (new SchemaInferrer(new SchemaInference(sampleSize: 10)))->infer($sniffer);

        static::assertSame([[0, 10]], $sniffer->sniffed);
    }

    public function test_a_column_that_is_free_text_in_the_first_source_and_numeric_in_the_second_widens_to_string(): void
    {
        $fixture = [
            [['id' => '1', 'v' => 'free text'], ['id' => '2', 'v' => 'more text']],
            [['id' => '3', 'v' => '42'], ['id' => '4', 'v' => '43']],
        ];
        $typer = new StringTypeNarrower(InferredTypes::default()->toArray());

        static::assertEquals(
            new Schema(
                definition_from_type('id', type_integer(), nullable: true),
                definition_from_type('v', type_string(), nullable: true),
            ),
            (new SchemaInferrer(new SchemaInference()))->infer(new FakeSniffer($fixture, $typer, ['id', 'v']))->schema,
        );
        static::assertEquals(
            definition_from_type('v', type_integer(), nullable: true),
            (new FakeSniffer($fixture, $typer, ['id', 'v']))
                ->sniff(1, null)
                ->columns
                ->schema(ColumnTypesMother::floor())
                ->get('v'),
        );
    }

    public function test_a_row_less_source_does_not_spend_a_files_to_sniff_slot_of_a_bounded_sample(): void
    {
        $sniffer = new FakeSniffer([
            [],
            [['a' => '1', 'b' => 'x']],
            [['c' => '1']],
        ], new StringTypeNarrower(InferredTypes::default()->toArray()));

        $schema = (new SchemaInferrer(new SchemaInference(100, 1)))->infer($sniffer)->schema;

        static::assertSame([0, 1], $sniffer->sources->started);
        static::assertSame(['a', 'b'], array_keys($schema->definitions()));
    }

    public function test_an_unbounded_sample_counts_empty_units_toward_files_to_sniff(): void
    {
        $sniffer = new FakeSniffer([
            [],
            [['a' => '1', 'b' => 'x']],
            [['c' => '1']],
        ], new StringTypeNarrower(InferredTypes::default()->toArray()));

        $schema = (new SchemaInferrer((new SchemaInference(filesToSniff: 2))->everyRow()))->infer($sniffer)->schema;

        static::assertSame([0, 1], $sniffer->sources->started);
        static::assertSame(['a', 'b'], array_keys($schema->definitions()));
    }

    public function test_an_unbounded_sample_sniffs_every_unit_whole(): void
    {
        $sniffer = new FakeSniffer(
            self::threeSourcesOfTenRows(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        (new SchemaInferrer((new SchemaInference())->everyRow()->everySource()))->infer($sniffer);

        static::assertSame([[0, null], [1, null], [2, null]], $sniffer->sniffed);
    }

    public function test_execution_order_does_not_change_the_schema(): void
    {
        $typer = new StringTypeNarrower(InferredTypes::default()->toArray());
        $inference = (new SchemaInference(unionByName: true))->everyRow()->everySource();

        foreach ([[], ['z', 'a']] as $names) {
            $inferred = (new SchemaInferrer($inference))->infer(
                new FakeSniffer(self::threeSourcesOfTenRows(), $typer, $names),
            )->schema;
            $reversed = ReversedSniffing::schema(
                new FakeSniffer(self::threeSourcesOfTenRows(), $typer, $names),
                $inference,
            );

            static::assertEquals($inferred, $reversed);
            static::assertSame(array_keys($inferred->definitions()), array_keys($reversed->definitions()));
        }
    }

    public function test_a_single_unit_is_accepted(): void
    {
        static::assertEquals(
            new Schema(definition_from_type('a', type_integer(), nullable: true)),
            (new SchemaInferrer(new SchemaInference()))->infer(new FakeSniffer([[[
                'a' => '1',
            ]]], new StringTypeNarrower(InferredTypes::default()->toArray())))->schema,
        );
    }

    public function test_a_sniffer_without_units_yields_every_header_name_as_nullable_string(): void
    {
        $inferrer = new SchemaInferrer(new SchemaInference());
        $typer = new StringTypeNarrower(InferredTypes::default()->toArray());

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_string(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $inferrer->infer(new FakeSniffer([], $typer, ['a', 'b']))->schema,
        );
        static::assertCount(0, $inferrer->infer(new FakeSniffer([], $typer))->schema->definitions());
    }

    /**
     * @param null|int<1, max> $sampleSize
     * @param array<string, string> $expected
     * @param array<int, int> $rowsRead
     */
    #[DataProvider('budgets')]
    public function test_the_budget_decides_how_much_of_the_sample_is_seen(
        ?int $sampleSize,
        array $expected,
        array $rowsRead,
    ): void {
        $sniffer = new FakeSniffer(
            self::threeSourcesOfTenRows(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        $schema = (new SchemaInferrer(new SchemaInference($sampleSize, unionByName: true)))->infer($sniffer)->schema;

        static::assertSame($rowsRead, $sniffer->sources->rowsRead);
        static::assertSame(array_keys($expected), array_keys($schema->definitions()));

        foreach ($expected as $name => $type) {
            static::assertSame($type, $schema->get($name)->type()->toString(), 'column ' . $name);
        }
    }

    public function test_infer_over_n_units_equals_the_partials_merged_in_listing_order(): void
    {
        $fixture = self::threeSourcesOfTenRows();
        $typer = new StringTypeNarrower(InferredTypes::default()->toArray());
        $inferrer = new SchemaInferrer(new SchemaInference(unionByName: true));

        foreach ([[], ['z', 'a']] as $names) {
            for ($count = 1; $count <= 3; $count++) {
                $merged = ColumnTypesMother::fromStrings($names)->folded();

                foreach (array_slice($fixture, 0, $count) as $rows) {
                    $merged = $merged->merge(ColumnTypes::sniff($names, $rows, null, $typer)->folded(), true);
                }

                $sequential = $inferrer->infer(
                    new FakeSniffer(array_slice($fixture, 0, $count), $typer, $names),
                )->schema;

                static::assertEquals($merged->schema(ColumnTypesMother::floor()), $sequential);
                static::assertSame(
                    array_keys($merged->schema(ColumnTypesMother::floor())->definitions()),
                    array_keys($sequential->definitions()),
                );
            }
        }
    }

    public function test_only_the_first_unit_is_opened_when_files_to_sniff_is_one(): void
    {
        $sniffer = new FakeSniffer(
            self::threeSourcesOfTenRows(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        (new SchemaInferrer((new SchemaInference(filesToSniff: 1))->everyRow()))->infer($sniffer);

        static::assertSame([0], $sniffer->sources->started);
        static::assertSame([0 => 10], $sniffer->sources->rowsRead);
    }

    public function test_types_outside_the_candidate_set_are_floored_to_string(): void
    {
        $schema = (new SchemaInferrer(new SchemaInference(
            types: new InferredTypes(type_string()),
            unionByName: true,
        )))->infer(new FakeSniffer(self::threeSourcesOfTenRows(), new StringTypeNarrower([type_string()])))->schema;

        foreach (['a', 'b', 'c'] as $name) {
            static::assertSame('string', $schema->get($name)->type()->toString(), 'column ' . $name);
        }
    }

    public function test_union_by_name_decides_whether_a_later_sources_column_joins_the_schema(): void
    {
        $typer = new StringTypeNarrower(InferredTypes::default()->toArray());

        $unioned = (new SchemaInferrer(new SchemaInference(unionByName: true)))->infer(
            new FakeSniffer(self::threeSourcesOfTenRows(), $typer),
        )->schema;
        $firstSourceWins = (new SchemaInferrer(new SchemaInference()))->infer(
            new FakeSniffer(self::threeSourcesOfTenRows(), $typer),
        )->schema;

        static::assertSame(['a', 'b', 'c'], array_keys($unioned->definitions()));
        static::assertSame(['a', 'b'], array_keys($firstSourceWins->definitions()));
    }

    public function test_the_candidate_set_reaches_the_floor_through_infer(): void
    {
        static::assertEquals(
            new Schema(
                definition_from_type('i', type_integer(), nullable: true),
                definition_from_type('f', type_string(), nullable: true),
                definition_from_type('d', type_string(), nullable: true),
            ),
            (new SchemaInferrer(new SchemaInference(types: new InferredTypes(type_integer(), type_string()))))->infer(
                new FakeSniffer([[[
                    'i' => '42',
                    'f' => '1.5',
                    'd' => '2024-01-01',
                ]]], new StringTypeNarrower([type_integer(), type_string()])),
            )->schema,
        );
    }

    public function test_union_by_name_over_sources_where_one_column_saturates_to_string(): void
    {
        static::assertEquals(
            new Schema(
                definition_from_type('id', type_integer(), nullable: true),
                definition_from_type('v', type_string(), nullable: true),
                definition_from_type('w', type_date(), nullable: true),
            ),
            (new SchemaInferrer(new SchemaInference(unionByName: true)))->infer(new FakeSniffer([
                [['id' => '1', 'v' => 'free text']],
                [['id' => '2', 'v' => '42', 'w' => '2024-01-01']],
            ], new StringTypeNarrower(InferredTypes::default()->toArray())))->schema,
        );
    }

    public function test_unbounded_bounds_read_every_row_of_every_unit(): void
    {
        $sniffer = new FakeSniffer(
            self::threeSourcesOfTenRows(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        (new SchemaInferrer((new SchemaInference())->everyRow()->everySource()))->infer($sniffer);

        static::assertSame([0, 1, 2], $sniffer->sources->started);
        static::assertSame([0 => 10, 1 => 10, 2 => 10], $sniffer->sources->rowsRead);
    }

    public function test_an_unbounded_sample_asks_the_launcher_once_and_sniffs_here_when_it_declines(): void
    {
        $launcher = new RecordingLauncher();
        $sniffer = new FakeSniffer(
            self::threeSourcesOfTenRows(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
            ['id'],
        );

        $inferred = (new SchemaInferrer((new SchemaInference(sniffs: $launcher))->everyRow()->everySource()))->infer(
            $sniffer,
        );

        static::assertCount(1, $launcher->sniffs);
        static::assertSame(3, $launcher->sniffs[0][1]);
        static::assertCount(3, $inferred->units->samples);
        static::assertEquals(
            (new SchemaInferrer((new SchemaInference())->everyRow()->everySource()))->infer($sniffer)->schema,
            $inferred->schema,
        );
    }

    public function test_files_to_sniff_caps_the_units_handed_to_the_launcher(): void
    {
        $launcher = new RecordingLauncher();
        $units = self::threeSourcesOfTenRows();
        $units[] = $units[0];

        (new SchemaInferrer((new SchemaInference(filesToSniff: 2, sniffs: $launcher))->everyRow()))->infer(
            new FakeSniffer($units, new StringTypeNarrower(InferredTypes::default()->toArray()), ['id']),
        );

        static::assertSame(2, $launcher->sniffs[0][1]);
    }

    public function test_a_bounded_sample_never_asks_the_launcher(): void
    {
        $launcher = new RecordingLauncher();

        (new SchemaInferrer(new SchemaInference(sampleSize: 5, sniffs: $launcher)))->infer(
            new FakeSniffer(
                self::threeSourcesOfTenRows(),
                new StringTypeNarrower(InferredTypes::default()->toArray()),
                ['id'],
            ),
        );

        static::assertSame([], $launcher->sniffs);
    }

    public function test_the_samples_the_launcher_returns_are_merged_in_the_order_returned(): void
    {
        $sniffer = new FakeSniffer(
            [[['a' => '1']], [['b' => 'x']]],
            new StringTypeNarrower(InferredTypes::default()->toArray()),
            [],
        );

        static::assertSame(
            ['b', 'a'],
            array_keys(
                (new SchemaInferrer((new SchemaInference(unionByName: true))->everyRow()))
                    ->infer(new PresniffedSniffer([], [$sniffer->sniff(1, null), $sniffer->sniff(0, null)]))
                    ->schema->definitions(),
            ),
        );
    }
}
