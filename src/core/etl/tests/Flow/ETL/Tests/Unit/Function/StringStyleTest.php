<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Function\StyleConverter\StringStyles as OldStringStyles;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\{DataProvider, IgnoreDeprecations};

#[IgnoreDeprecations]
final class StringStyleTest extends FlowTestCase
{
    /**
     * @return iterable<array-key, mixed>
     */
    public static function provideStringStyles() : iterable
    {
        yield 'null new' => [
            StringStyles::LOWER,
            null,
            null,
        ];

        yield 'null old' => [
            OldStringStyles::LOWER,
            null,
            null,
        ];

        yield 'camel new' => [
            StringStyles::CAMEL,
            'Foo: Bar-baz.',
            'fooBarBaz',
        ];

        yield 'camel old' => [
            OldStringStyles::CAMEL,
            'Foo: Bar-baz.',
            'fooBarBaz',
        ];

        yield 'snake new' => [
            StringStyles::SNAKE,
            'Foo: Bar-baz.',
            'foo_bar_baz',
        ];

        yield 'snake old' => [
            OldStringStyles::SNAKE,
            'Foo: Bar-baz.',
            'foo_bar_baz',
        ];

        yield 'title new' => [
            StringStyles::TITLE,
            'foo ijssel',
            'Foo ijssel',
        ];

        yield 'title old' => [
            OldStringStyles::TITLE,
            'foo ijssel',
            'Foo ijssel',
        ];

        yield 'upper new' => [
            StringStyles::UPPER,
            'foo ijssel',
            'FOO IJSSEL',
        ];

        yield 'upper old' => [
            OldStringStyles::UPPER,
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

    #[DataProvider('provideStringStyles')]
    public function test_string_styles(
        OldStringStyles|StringStyles $style,
        ?string $value,
        ?string $expected,
    ) : void {
        self::assertSame(
            $expected,
            ref('str')->stringStyle($style)->eval(
                row(
                    str_entry('str', $value),
                )
            )
        );
    }
}
