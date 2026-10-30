<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\batch_by;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function iterator_to_array;

final class BatchByTest extends FlowTestCase
{
    public function test_batch_by_keeps_rows_of_one_value_in_one_batch(): void
    {
        static::assertSame(
            [
                [['order_id' => 1], ['order_id' => 1], ['order_id' => 2], ['order_id' => 2]],
                [['order_id' => 3]],
            ],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    df()
                        ->read(from_array([
                            ['order_id' => 1],
                            ['order_id' => 1],
                            ['order_id' => 2],
                            ['order_id' => 2],
                            ['order_id' => 3],
                        ]))
                        ->batchSize(1)
                        ->with(batch_by(ref('order_id'), 3))
                        ->get(),
                    false,
                ),
            ),
        );
    }

    public function test_a_minimum_size_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        batch_by('order_id', 0);
    }
}
