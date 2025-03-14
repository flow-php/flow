<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\{StringAfterLast};
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringAfterLastTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringAfterLastFunction = new StringAfterLast('test', 'e');
        $returnType = $stringAfterLastFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_after_last() : void
    {
        self::assertSame(
            'rld',
            ref('str')->stringAfterLast(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_last_including_needle() : void
    {
        self::assertSame(
            'orld',
            ref('str')->stringAfterLast(ref('needle'), includeNeedle: true)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_after_last_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringAfterLast('x')->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
