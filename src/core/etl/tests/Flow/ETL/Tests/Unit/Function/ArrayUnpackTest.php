<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayUnpackTest extends TestCase
{
    public function test_array_unpack() : void
    {
        $row = Row::create(
            int_entry('id', 1),
            array_entry('array_entry', [
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ]),
        );

        $this->assertSame(
            [
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['foo' => 'bar'],
            ],
            (new ArrayUnpack(ref('array_entry')))->eval($row)
        );
    }
}
