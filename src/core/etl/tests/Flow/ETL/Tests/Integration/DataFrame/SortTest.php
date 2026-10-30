<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Memory\Unit;
use Flow\Types\Value\Uuid;

use function array_column;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;
use function Flow\ETL\DSL\xml_schema;
use function range;

final class SortTest extends FlowIntegrationTestCase
{
    public function test_a_bare_reference_sorts_like_a_one_element_array(): void
    {
        $dataset = [['id' => 3], ['id' => 1], ['id' => 2]];

        static::assertSame(
            df()
                ->read(from_array($dataset))
                ->sortBy([ref('id')])
                ->fetch()
                ->reduceToArray('id'),
            df()->read(from_array($dataset))->sortBy(ref('id'))->fetch()->reduceToArray('id'),
        );
    }

    public function test_a_bare_reference_honours_its_sort_order(): void
    {
        static::assertSame(
            [3, 2, 1],
            df()
                ->read(from_array([['id' => 1], ['id' => 3], ['id' => 2]]))
                ->sortBy(ref('id')->desc())
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nulls_go_first_ascending_and_last_descending_by_default(): void
    {
        $dataset = [['v' => 2], ['v' => null], ['v' => 1]];

        static::assertSame(
            [null, 1, 2],
            df()->read(from_array($dataset))->sortBy(ref('v'))->fetch()->reduceToArray('v'),
        );
        static::assertSame(
            [null, 1, 2],
            df()->read(from_array($dataset))->sortBy(ref('v')->asc())->fetch()->reduceToArray('v'),
        );
        static::assertSame(
            [2, 1, null],
            df()->read(from_array($dataset))->sortBy(ref('v')->desc())->fetch()->reduceToArray('v'),
        );
        static::assertSame(
            [1, 2, 3],
            df()
                ->read(from_array($dataset))
                ->withColumn('rank', rank()->over(window()->orderBy(ref('v'))))
                ->sortBy(ref('rank'))
                ->fetch()
                ->reduceToArray('rank'),
        );
        static::assertSame(
            [null, 1, 2],
            df()
                ->read(from_array($dataset))
                ->withColumn('rank', rank()->over(window()->orderBy(ref('v'))))
                ->sortBy(ref('rank'))
                ->fetch()
                ->reduceToArray('v'),
        );
    }

    public function test_etl_sort_by_external_sort(): void
    {
        static::assertSame(
            range(0, 99),
            df(config_builder()->sort(external_sort()->memoryLimit(Unit::fromBytes(1)))->build())
                ->read(new FakeExtractor(100))
                ->batchSize(20)
                ->sortBy([ref('int')])
                ->fetch()
                ->reduceToArray('int'),
        );
    }

    public function test_etl_sort_by_in_memory(): void
    {
        static::assertSame(
            range(0, 39),
            df(config_builder()->sort(memory_sort())->build())
                ->read(new FakeExtractor(40))
                ->batchSize(2)
                ->sortBy([ref('int')])
                ->fetch()
                ->reduceToArray('int'),
        );
    }

    public function test_sorting_by_an_arithmetic_column_does_not_break_the_spill_schema(): void
    {
        $rows = data_frame()
            ->read(from_array([
                ['id' => 0, 'price' => 1.0],
                ['id' => 1, 'price' => 1.1],
                ['id' => 2, 'price' => 2.0],
                ['id' => 3, 'price' => 2.2],
            ]))
            ->withColumn('total', ref('price')->multiply(lit(2)))
            ->sortBy([ref('total')])
            ->fetch();

        static::assertSame('float', $rows->schema()->get('total')->type()->toString());
        static::assertSame([2.0, 2.2, 4.0, 4.4], $rows->reduceToArray('total'));
    }

    public function test_uuids_sort_by_their_bytes(): void
    {
        static::assertSame(
            [2, 3, 1],
            df()
                ->read(from_array([
                    ['id' => 1, 'u' => new Uuid('ffffffff-0000-4000-8000-000000000000')],
                    ['id' => 2, 'u' => new Uuid('00000000-0000-4000-8000-000000000000')],
                    ['id' => 3, 'u' => new Uuid('88888888-0000-4000-8000-000000000000')],
                ]))
                ->sortBy(ref('u'))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_json_sorts_by_its_text(): void
    {
        static::assertSame(
            [2, 3, 1],
            df()
                ->read(from_array(
                    [['id' => 1, 'j' => '{"c":1}'], ['id' => 2, 'j' => '{"a":1}'], ['id' => 3, 'j' => '{"b":1}']],
                    schema(int_schema('id'), json_schema('j')),
                ))
                ->sortBy(ref('j'))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_xml_sorts_by_its_document(): void
    {
        static::assertSame(
            [2, 3, 1],
            df()
                ->read(from_array(
                    [['id' => 1, 'x' => '<c/>'], ['id' => 2, 'x' => '<a/>'], ['id' => 3, 'x' => '<b/>']],
                    schema(int_schema('id'), xml_schema('x')),
                ))
                ->sortBy(ref('x'))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_enums_sort_by_case_name(): void
    {
        static::assertSame(
            [2, 3, 1],
            df()
                ->read(from_array(
                    [
                        ['id' => 1, 'e' => BasicEnum::two],
                        ['id' => 2, 'e' => BasicEnum::one],
                        ['id' => 3, 'e' => BasicEnum::three],
                    ],
                    schema(int_schema('id'), enum_schema('e', BasicEnum::class)),
                ))
                ->sortBy(ref('e'))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_a_window_ranks_uuids_by_their_bytes(): void
    {
        static::assertEquals(
            [1 => 3, 2 => 1, 3 => 2],
            array_column(
                df()
                    ->read(from_array([
                        ['id' => 1, 'g' => 1, 'u' => new Uuid('ffffffff-0000-4000-8000-000000000000')],
                        ['id' => 2, 'g' => 1, 'u' => new Uuid('00000000-0000-4000-8000-000000000000')],
                        ['id' => 3, 'g' => 1, 'u' => new Uuid('88888888-0000-4000-8000-000000000000')],
                    ]))
                    ->withColumn('rank', rank()->over(window()->partitionBy(ref('g'))->orderBy(ref('u'))))
                    ->fetch()
                    ->toArray(),
                'rank',
                'id',
            ),
        );
    }
}
