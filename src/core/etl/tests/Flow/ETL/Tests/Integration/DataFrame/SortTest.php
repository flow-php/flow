<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function range;

final class SortTest extends FlowIntegrationTestCase
{
    public function test_etl_sort_by_external_sort(): void
    {
        $config = config_builder()->sort(external_sort()->runSize(100));

        $rows = df($config->build())->read(new FakeExtractor(2500))->batchSize(50)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 2499), $rows->reduceToArray('int'));
    }

    public function test_etl_sort_by_in_memory(): void
    {
        $config = config_builder()->sort(memory_sort());

        $rows = df($config->build())->read(new FakeExtractor(40))->batchSize(2)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 39), $rows->reduceToArray('int'));
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
            ->withEntry('total', ref('price')->multiply(lit(2)))
            ->sortBy(ref('total'))
            ->fetch();

        static::assertSame('float', $rows->schema()->get('total')->type()->toString());
        static::assertSame([2.0, 2.2, 4.0, 4.4], $rows->reduceToArray('total'));
    }
}
