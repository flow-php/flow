<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of(): void
    {
        static::assertSame(5, ref('str')
            ->indexOf('x', offset: 5)
            ->eval(row(str_entry('str', 'AbBAsxa')), flow_context()));

        static::assertSame(0, ref('str')
            ->indexOf('A', ignoreCase: true)
            ->eval(row(str_entry('str', 'abbbbb')), flow_context()));

        static::assertSame(5, ref('str')
            ->indexOf('x', offset: 5)
            ->eval(row(str_entry('str', 'AbBAsxa')), flow_context()));

        static::assertNull(ref('str')->indexOf('x', offset: 2)->eval(row(str_entry('str', 'Abba')), flow_context()));
    }

    public function test_index_of_throws_on_null_needle(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOf function requires non-null string and needle');

        ref('str')->indexOf(ref('needle'))->eval(row(str_entry('str', 'x'), str_entry('needle', null)), flow_context());
    }

    public function test_index_of_throws_on_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOf function requires non-null string and needle');

        ref('str')->indexOf('x')->eval(row(str_entry('str', null)), flow_context());
    }
}
