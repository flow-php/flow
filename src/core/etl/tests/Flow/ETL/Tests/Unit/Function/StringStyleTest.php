<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\StringStyle;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;

final class StringStyleTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringStyleFunction = new StringStyle('str', StringStyles::CAMEL);
        $returnType = $stringStyleFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_style_camel() : void
    {
        self::assertSame(
            'fooBarBaz',
            ref('str')->stringStyle(ref('style'))->eval(
                row(
                    str_entry('str', 'Foo: Bar-baz.'),
                    str_entry('style', 'camel')
                )
            )
        );
    }

    public function test_string_style_kebab() : void
    {
        self::assertSame(
            'foo-bar-baz',
            ref('str')->stringStyle('kebab')->eval(
                row(str_entry('str', 'Foo: Bar-baz.'))
            )
        );
    }

    public function test_string_style_lower() : void
    {
        self::assertSame(
            'foo bar bri̇an',
            ref('str')->stringStyle('lower')->eval(
                row(str_entry('str', 'FOO Bar Brİan'))
            )
        );
    }

    public function test_string_style_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringStyle(StringStyles::LOWER)->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }

    public function test_string_style_snake() : void
    {
        self::assertSame(
            'foo_bar_baz',
            ref('str')->stringStyle(StringStyles::SNAKE)->eval(
                row(str_entry('str', 'Foo: Bar-baz.'))
            )
        );
    }

    public function test_string_style_title() : void
    {
        self::assertSame(
            'Foo ijssel',
            ref('str')->stringStyle(StringStyles::TITLE)->eval(
                row(str_entry('str', 'foo ijssel'))
            )
        );
    }

    public function test_string_style_upper() : void
    {
        self::assertSame(
            'FOO BAR BΆZ',
            ref('str')->stringStyle(StringStyles::UPPER)->eval(
                row(str_entry('str', 'foo BAR bάz'))
            )
        );
    }
}
