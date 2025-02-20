<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\{StringAfter};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringAfterTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringAfterFunction = new StringAfter('test', 'e');
        $returnType = $stringAfterFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_after() : void
    {
        self::assertSame(
            ' world',
            ref('str')->stringAfter(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'hello')
                )
            )
        );

        self::assertSame(
            ' world',
            ref('str')->stringAfter(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_including_needle() : void
    {
        self::assertSame(
            'o world',
            ref('str')->stringAfter(ref('needle'), includeNeedle: true)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringAfter('x')->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
