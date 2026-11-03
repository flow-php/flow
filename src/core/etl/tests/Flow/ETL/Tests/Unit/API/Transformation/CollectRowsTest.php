<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\collect_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function iterator_to_array;

final class CollectRowsTest extends FlowTestCase
{
    public function test_collect_rows_through_invoke(): void
    {
        static::assertCount(1, iterator_to_array(
            collect_rows()(df()->read(from_array([['a' => 1], ['a' => 2]]))->batchSize(1))->get(),
            false,
        ));
    }

    public function test_collect_rows_through_with(): void
    {
        static::assertCount(1, iterator_to_array(
            df()
                ->read(from_array([['a' => 1], ['a' => 2]]))
                ->batchSize(1)
                ->with(collect_rows())
                ->get(),
            false,
        ));
    }
}
