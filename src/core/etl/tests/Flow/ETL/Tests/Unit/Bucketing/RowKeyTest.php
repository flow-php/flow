<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\RowKey;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;

final class RowKeyTest extends FlowTestCase
{
    public function test_exposes_row_and_values(): void
    {
        $row = row(int_entry('id', 1));
        $key = new RowKey($row, [1]);

        static::assertSame($row, $key->row);
        static::assertSame([1], $key->values);
    }
}
