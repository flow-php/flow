<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\count_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;

final class CountRowsTest extends FlowTestCase
{
    public function test_count_rows_counts_the_rows(): void
    {
        static::assertSame(2, count_rows()(df()->read(from_array([['a' => 1], ['a' => 2]]))));
    }
}
