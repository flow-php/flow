<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_boolean};
use Flow\ETL\Function\IsUtf8;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class IsUtf8Test extends FlowTestCase
{
    public function test_is_utf_8() : void
    {
        self::assertTrue(
            ref('str')->isUtf8()->eval(
                row(str_entry('str', 'Lorem Ipsum'))
            )
        );

        self::assertFalse(
            ref('str')->isUtf8()->eval(
                row(str_entry('str', "\xc3\x28"))
            )
        );
    }

    public function test_returns_method_returns_string_boolean() : void
    {
        $isUtf8Function = new IsUtf8('Lorem Ipsum');
        $returnType = $isUtf8Function->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_boolean()));
    }
}
