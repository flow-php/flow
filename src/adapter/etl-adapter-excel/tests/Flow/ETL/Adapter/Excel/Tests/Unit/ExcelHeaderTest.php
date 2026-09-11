<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\ExcelHeader;
use Flow\ETL\Tests\FlowTestCase;

final class ExcelHeaderTest extends FlowTestCase
{
    public function test_a_header_carries_the_names_and_the_file_they_came_from(): void
    {
        $header = new ExcelHeader(['id', 'name'], 'file://orders.xlsx');

        static::assertSame(['id', 'name'], $header->names);
        static::assertSame('file://orders.xlsx', $header->source);
    }

    public function test_a_header_with_no_names_has_no_source(): void
    {
        static::assertSame([], (new ExcelHeader([], null))->names);
        static::assertNull((new ExcelHeader([], null))->source);
    }
}
