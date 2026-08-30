<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringStyleTest extends FlowTestCase
{
    public function test_a_null_value_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringStyle function requires non-null value');

        ref('value')->stringStyle(StringStyles::LOWER)->eval(row(['value' => null]), flow_context());
    }

    /**
     * @return iterable<array-key, mixed>
     */
    public static function provideStringStyles(): iterable
    {
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

    public function test_string_style_camel(): void
    {
        static::assertSame('fooBarBaz', ref('str')
            ->stringStyle(ref('style'))
            ->eval(row(['str' => 'Foo: Bar-baz.', 'style' => 'camel']), flow_context()));
    }

    public function test_string_style_kebab(): void
    {
        static::assertSame('foo-bar-baz', ref('str')
            ->stringStyle('kebab')
            ->eval(row(['str' => 'Foo: Bar-baz.']), flow_context()));
    }

    public function test_string_style_lower(): void
    {
        static::assertSame('foo bar bri̇an', ref('str')
            ->stringStyle('lower')
            ->eval(row(['str' => 'FOO Bar Brİan']), flow_context()));
    }

    #[DataProvider('provideStringStyles')]
    public function test_string_styles(StringStyles $style, ?string $value, ?string $expected): void
    {
        static::assertSame($expected, ref('str')->stringStyle($style)->eval(row(['str' => $value]), flow_context()));
    }
}
