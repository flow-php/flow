<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class StringStyleTest extends FlowTestCase
{
    /**
     * @return iterable<array-key, mixed>
     */
    public static function provideStringStyles() : iterable
    {
        yield 'null' => [
            StringStyles::LOWER,
            null,
            null,
        ];

        yield 'camel' => [
            StringStyles::CAMEL,
            'Foo: Bar-baz.',
            'fooBarBaz',
        ];

        yield 'snake' => [
            StringStyles::SNAKE,
            'Foo: Bar-baz.',
            'foo_bar_baz',
        ];

        yield 'title' => [
            StringStyles::TITLE,
            'foo ijssel',
            'Foo ijssel',
        ];

        yield 'upper' => [
            StringStyles::UPPER,
            'foo ijssel',
            'FOO IJSSEL',
        ];
    }

    public function test_string_style_camel() : void
    {
        self::assertSame(
            'fooBarBaz',
            ref('str')->stringStyle(ref('style'))->eval(
                row(
                    str_entry('str', 'Foo: Bar-baz.'),
                    str_entry('style', 'camel')
                ),
                flow_context()
            )
        );
    }

    public function test_string_style_kebab() : void
    {
        self::assertSame(
            'foo-bar-baz',
            ref('str')->stringStyle('kebab')->eval(
                row(str_entry('str', 'Foo: Bar-baz.')),
                flow_context()
            )
        );
    }

    public function test_string_style_lower() : void
    {
        self::assertSame(
            'foo bar bri̇an',
            ref('str')->stringStyle('lower')->eval(
                row(str_entry('str', 'FOO Bar Brİan')),
                flow_context()
            )
        );
    }

    #[DataProvider('provideStringStyles')]
    public function test_string_styles(
        StringStyles $style,
        ?string $value,
        ?string $expected,
    ) : void {
        self::assertSame(
            $expected,
            ref('str')->stringStyle($style)->eval(
                row(
                    str_entry('str', $value),
                ),
                flow_context()
            )
        );
    }
}
