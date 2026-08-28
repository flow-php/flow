<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayUnpackTest extends FlowTestCase
{
    public function test_array_unpack(): void
    {
        $row = row(
            int_entry('id', 1),
            json_entry('array_entry', [
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );

        static::assertSame(
            [
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ],
            (new ArrayUnpack(ref('array_entry')))->eval($row, flow_context()),
        );
    }

    public function test_array_unpack_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayUnpack requires non-null array and skipKeys');

        $context = flow_context(config());
        (new ArrayUnpack(ref('array_entry')))->eval(row(int_entry('id', 1), json_entry('array_entry', null)), $context);
    }
}
