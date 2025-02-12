<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_int};
use Flow\ETL\Function\IndexOf;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of() : void
    {
        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa'))
            )
        );

        self::assertSame(
            0,
            ref('str')->indexOf('A', ignoreCase: true)->eval(
                row(str_entry('str', 'abbbbb'))
            )
        );

        self::assertSame(
            5,
            ref('str')->indexOf('x', offset: 5)->eval(
                row(str_entry('str', 'AbBAsxa'))
            )
        );

        self::assertNull(
            ref('str')->indexOf('x', offset: 2)->eval(
                row(str_entry('str', 'Abba'))
            )
        );
    }

    public function test_returns_method_returns_string_int() : void
    {
        $indexOf = new IndexOf('Abba', 'b', offset: 3);
        $returnType = $indexOf->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_int()));
    }
}
