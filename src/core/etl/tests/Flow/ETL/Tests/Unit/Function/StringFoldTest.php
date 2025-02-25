<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\StringFold;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringFoldTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringFoldedFunction = new StringFold('str');
        $returnType = $stringFoldedFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_folded() : void
    {
        self::assertSame(
            "die o'brian strasse",
            ref('str')->stringFold()->eval(
                row(str_entry('str', "Die O'Brian Straße"))
            )
        );
    }

    public function test_string_folded_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringFold()->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }
}
