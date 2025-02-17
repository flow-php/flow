<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\StringTitle;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringBeforeTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringTitleFunction = new StringTitle('str');
        $returnType = $stringTitleFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_before() : void
    {
        self::assertSame(
            'hello ',
            ref('str')->stringBefore(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'world')
                )
            )
        );

        self::assertSame(
            'hell',
            ref('str')->stringBefore(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }

    public function test_string_before_including_needle() : void
    {
        self::assertSame(
            'hello',
            ref('str')->stringBefore(ref('needle'), includeNeedle: true)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'o')
                )
            )
        );
    }
}
