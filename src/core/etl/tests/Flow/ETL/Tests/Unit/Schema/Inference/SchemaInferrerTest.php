<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\ETL\Tests\Double\FakeSchemaSampler;
use Flow\ETL\Tests\Double\RecordingSources;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function array_slice;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class SchemaInferrerTest extends FlowTestCase
{
    /**
     * The budget decides how much of the heterogeneous fixture is seen, so each row is a different schema.
     *
     * @return array<string, array{int<1, max>|-1, array<string, string>, array<int, int>}>
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
            'unbounded' => [-1, ['a' => 'float', 'b' => 'string', 'c' => 'boolean'], [0 => 10, 1 => 10, 2 => 10]],
        ];
    }

    /**
     * Heterogeneous on purpose: "a" widens integer -> float inside source 1, and source 2 introduces "c", so a
     * budget that stops early and a merge that drops a name are both observable.
     *
     * @return list<list<RawRowValues>>
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

                $rows[] = new RawRowValues($values);
            }

            $sources[] = $rows;
        }

        return $sources;
    }

    public function test_a_row_less_source_does_not_spend_a_files_to_sniff_slot(): void
    {
        $sources = new RecordingSources([
            [],
            [new RawRowValues(['a' => '1', 'b' => 'x'])],
            [new RawRowValues(['c' => '1'])],
        ]);

        $schema = (new SchemaInferrer(
            new SchemaInference(-1, 1),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], $sources->sources());

        static::assertSame([0, 1], $sources->started);
        static::assertSame(['a', 'b'], array_keys($schema->definitions()));
    }

    public function test_a_single_generator_source_is_accepted(): void
    {
        $sources = new RecordingSources([[new RawRowValues(['a' => '1'])]]);

        static::assertEquals(
            new Schema(definition_from_type('a', type_integer(), nullable: true)),
            (new SchemaInferrer(
                new SchemaInference(),
                new StringTypeNarrower(InferredTypes::default()->toArray()),
            ))->infer([], [$sources->source(0)]),
        );
    }

    public function test_an_empty_outer_iterable_yields_every_header_name_as_nullable_string(): void
    {
        $inferrer = new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        static::assertEquals(
            new Schema(
                definition_from_type('a', type_string(), nullable: true),
                definition_from_type('b', type_string(), nullable: true),
            ),
            $inferrer->infer(['a', 'b'], []),
        );
        static::assertCount(0, $inferrer->infer([], [])->definitions());
    }

    /**
     * @param int<1, max>|-1 $sampleSize
     * @param array<string, string> $expected
     * @param array<int, int> $rowsRead
     */
    #[DataProvider('budgets')]
    public function test_the_budget_decides_how_much_of_the_sample_is_seen(
        int $sampleSize,
        array $expected,
        array $rowsRead,
    ): void {
        $sources = new RecordingSources(self::threeSourcesOfTenRows());

        $schema = (new SchemaInferrer(
            new SchemaInference($sampleSize, unionByName: true),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], $sources->sources());

        static::assertSame($rowsRead, $sources->rowsRead);
        static::assertSame(array_keys($expected), array_keys($schema->definitions()));

        foreach ($expected as $name => $type) {
            static::assertSame($type, $schema->get($name)->type()->toString(), 'column ' . $name);
        }
    }

    public function test_infer_over_n_sources_equals_the_partials_merged_in_listing_order(): void
    {
        $fixture = self::threeSourcesOfTenRows();
        $inferrer = new SchemaInferrer(
            new SchemaInference(unionByName: true),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        foreach ([[], ['z', 'a']] as $names) {
            for ($count = 1; $count <= 3; $count++) {
                $merged = ColumnTypesMother::fromStrings($names);

                foreach ((new RecordingSources(array_slice($fixture, 0, $count)))->sources() as $source) {
                    $merged = $merged->merge($inferrer->sniff($names, $source, -1), true);
                }

                $sequential = $inferrer->infer(
                    $names,
                    (new RecordingSources(array_slice($fixture, 0, $count)))->sources(),
                );

                static::assertEquals($merged->schema(ColumnTypesMother::floor()), $sequential);
                static::assertSame(
                    array_keys($merged->schema(ColumnTypesMother::floor())->definitions()),
                    array_keys($sequential->definitions()),
                );
            }
        }
    }

    public function test_only_the_first_source_is_opened_when_files_to_sniff_is_one(): void
    {
        $sources = new RecordingSources(self::threeSourcesOfTenRows());

        (new SchemaInferrer(
            new SchemaInference(-1, 1),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], $sources->sources());

        static::assertSame([0], $sources->started);
        static::assertSame([0 => 10], $sources->rowsRead);
    }

    public function test_the_sampler_contract_is_a_name_for_the_shape_infer_already_consumes(): void
    {
        $fixture = self::threeSourcesOfTenRows();
        $inference = new SchemaInference(15);
        $inferrer = new SchemaInferrer($inference, new StringTypeNarrower(InferredTypes::default()->toArray()));
        $sampler = new FakeSchemaSampler($fixture);

        $viaSampler = $inferrer->infer(['a', 'b'], $sampler->samples($inference->sampleSize));
        $viaSources = $inferrer->infer(['a', 'b'], (new RecordingSources($fixture))->sources());

        static::assertEquals($viaSources, $viaSampler);
        static::assertSame(array_keys($viaSources->definitions()), array_keys($viaSampler->definitions()));
        static::assertSame([15], $sampler->askedBudgets);
    }

    public function test_types_outside_the_candidate_set_are_floored_to_string(): void
    {
        $sources = new RecordingSources(self::threeSourcesOfTenRows());

        $schema = (new SchemaInferrer(
            new SchemaInference(types: new InferredTypes(type_string()), unionByName: true),
            new StringTypeNarrower([type_string()]),
        ))->infer([], $sources->sources());

        foreach (['a', 'b', 'c'] as $name) {
            static::assertSame('string', $schema->get($name)->type()->toString(), 'column ' . $name);
        }
    }

    public function test_union_by_name_decides_whether_a_later_sources_column_joins_the_schema(): void
    {
        $fixture = self::threeSourcesOfTenRows();

        $unioned = (new SchemaInferrer(
            new SchemaInference(unionByName: true),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], (new RecordingSources($fixture))->sources());

        $firstSourceWins = (new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], (new RecordingSources($fixture))->sources());

        static::assertSame(['a', 'b', 'c'], array_keys($unioned->definitions()));
        static::assertSame(['a', 'b'], array_keys($firstSourceWins->definitions()));
    }

    public function test_sniff_observes_at_most_its_budget_and_opens_only_its_own_source(): void
    {
        $sources = new RecordingSources(self::threeSourcesOfTenRows());
        $inferrer = new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        );

        // the siblings exist and stay untouched, so $started proves sniff() opened only the one it was handed
        $first = $sources->source(0);
        $sources->source(1);
        $sources->source(2);

        $partial = $inferrer->sniff([], $first, 4);

        static::assertSame(4, $partial->rows());
        static::assertSame([0], $sources->started);
        static::assertSame([0 => 4], $sources->rowsRead);
    }

    public function test_sniff_seeds_the_names_it_is_handed_even_when_no_row_carries_them(): void
    {
        $partial = (new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->sniff(['ghost'], [new RawRowValues(['a' => '1'])], -1);

        static::assertSame(['ghost', 'a'], array_keys($partial->schema(ColumnTypesMother::floor())->definitions()));
        static::assertSame('string', $partial->schema(ColumnTypesMother::floor())->get('ghost')->type()->toString());
    }

    public function test_sniff_observes_nothing_when_the_budget_is_already_spent(): void
    {
        $sources = new RecordingSources([[new RawRowValues(['a' => '1'])]]);

        $partial = (new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->sniff([], $sources->source(0), 0);

        static::assertSame(0, $partial->rows());
        static::assertSame([], $sources->rowsRead);
    }

    public function test_sniff_returns_a_partial_equal_to_a_bare_fold_of_the_same_rows(): void
    {
        $rows = self::threeSourcesOfTenRows()[0];
        $bare = ColumnTypesMother::fromStrings();

        foreach ($rows as $row) {
            $bare->observe($row);
        }

        $partial = (new SchemaInferrer(
            new SchemaInference(),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->sniff([], (new RecordingSources([$rows]))->source(0), -1);

        static::assertSame(10, $partial->rows());
        static::assertEquals($bare->schema(ColumnTypesMother::floor()), $partial->schema(ColumnTypesMother::floor()));
    }

    public function test_the_candidate_set_reaches_the_floor_through_infer(): void
    {
        $schema = (new SchemaInferrer(
            new SchemaInference(types: new InferredTypes(type_integer(), type_string())),
            new StringTypeNarrower([type_integer(), type_string()]),
        ))->infer([], [[new RawRowValues(['i' => '42', 'f' => '1.5', 'd' => '2024-01-01'])]]);

        static::assertEquals(
            new Schema(
                definition_from_type('i', type_integer(), nullable: true),
                definition_from_type('f', type_string(), nullable: true),
                definition_from_type('d', type_string(), nullable: true),
            ),
            $schema,
        );
    }

    public function test_unbounded_bounds_read_every_row_of_every_source(): void
    {
        $sources = new RecordingSources(self::threeSourcesOfTenRows());

        (new SchemaInferrer(
            new SchemaInference(-1, -1),
            new StringTypeNarrower(InferredTypes::default()->toArray()),
        ))->infer([], $sources->sources());

        static::assertSame([0, 1, 2], $sources->started);
        static::assertSame([0 => 10, 1 => 10, 2 => 10], $sources->rowsRead);
    }
}
