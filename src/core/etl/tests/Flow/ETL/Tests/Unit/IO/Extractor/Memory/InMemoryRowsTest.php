<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\Memory;

use Flow\ETL\IO\Extractor\Memory\InMemoryRows;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function iterator_to_array;

final class InMemoryRowsTest extends FlowTestCase
{
    public function test_it_reads_the_source_again_on_every_call(): void
    {
        $rows = new InMemoryRows([['id' => 1], ['id' => 2]]);

        static::assertEquals([['id' => 1], ['id' => 2]], iterator_to_array($rows->values(), false));
        static::assertEquals([['id' => 1], ['id' => 2]], iterator_to_array($rows->values(), false));
    }

    public function test_positional_keys_are_named_like_the_hydrator_names_them(): void
    {
        static::assertSame(
            ['e00', 'e01'],
            array_keys(iterator_to_array((new InMemoryRows([[1, 2]]))->values(), false)[0]),
        );
    }

    public function test_the_dataset_is_one_unit_without_a_header(): void
    {
        static::assertSame(1, (new InMemoryRows([['id' => 1]]))->units());
        static::assertSame([], (new InMemoryRows([['id' => 1]]))->names());
    }

    public function test_sniff_folds_at_most_its_budget(): void
    {
        static::assertSame(1, (new InMemoryRows([['id' => 1], ['id' => 2], ['id' => 3]]))->sniff(0, 1)->columns->rows);
        static::assertSame(
            3,
            (new InMemoryRows([['id' => 1], ['id' => 2], ['id' => 3]]))->sniff(0, null)->columns->rows,
        );
    }
}
