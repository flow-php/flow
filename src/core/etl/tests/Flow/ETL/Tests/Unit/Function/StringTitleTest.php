<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\StringTitle;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringTitleTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringTitleFunction = new StringTitle('str');
        $returnType = $stringTitleFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_title() : void
    {
        self::assertSame(
            'Foo ijssel',
            ref('str')->stringTitle()->eval(
                row(str_entry('str', 'foo ijssel'))
            )
        );
    }

    public function test_string_title_allwords() : void
    {
        self::assertSame(
            'Foo Ijssel',
            ref('str')->stringTitle(allWords: true)->eval(
                row(str_entry('str', 'foo ijssel'))
            )
        );
    }
}
