<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\InMemoryRows;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function iterator_to_array;

final class InMemoryRowsTest extends FlowTestCase
{
    public function test_it_reads_the_source_again_on_every_call(): void
    {
        $rows = new InMemoryRows([['id' => 1], ['id' => 2]]);

        static::assertEquals(
            [new RawRowValues(['id' => 1]), new RawRowValues(['id' => 2])],
            iterator_to_array($rows->values(), false),
        );
        static::assertEquals(
            [new RawRowValues(['id' => 1]), new RawRowValues(['id' => 2])],
            iterator_to_array($rows->values(), false),
        );
    }

    public function test_positional_keys_are_named_like_the_hydrator_names_them(): void
    {
        static::assertSame(
            ['e00', 'e01'],
            array_keys(iterator_to_array((new InMemoryRows([[1, 2]]))->values(), false)[0]->values),
        );
    }

    public function test_samples_yields_exactly_one_inner_iterable(): void
    {
        static::assertCount(1, iterator_to_array((new InMemoryRows([['id' => 1]]))->samples(-1), false));
    }

    public function test_the_row_budget_is_never_rationed(): void
    {
        $units = iterator_to_array((new InMemoryRows([['id' => 1], ['id' => 2], ['id' => 3]]))->samples(1), false);

        static::assertCount(3, iterator_to_array($units[0], false));
    }
}
