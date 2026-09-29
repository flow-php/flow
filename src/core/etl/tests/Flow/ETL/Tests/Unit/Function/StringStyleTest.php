<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringStyleTest extends FlowTestCase
{
    public function test_a_null_value_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringStyle function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->stringStyle(StringStyles::LOWER),
            ['value' => null],
            schema(str_schema('value', nullable: true)),
        );
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
        static::assertSame('fooBarBaz', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringStyle(ref('style')),
            ['str' => 'Foo: Bar-baz.', 'style' => 'camel'],
            schema(str_schema('str'), str_schema('style')),
        ));
    }

    public function test_string_style_kebab(): void
    {
        static::assertSame('foo-bar-baz', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringStyle('kebab'),
            ['str' => 'Foo: Bar-baz.'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_style_lower(): void
    {
        static::assertSame('foo bar bri̇an', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringStyle('lower'),
            ['str' => 'FOO Bar Brİan'],
            schema(str_schema('str')),
        ));
    }

    #[DataProvider('provideStringStyles')]
    public function test_string_styles(StringStyles $style, ?string $value, ?string $expected): void
    {
        static::assertSame($expected, (new FunctionContext(flow_context()))->eval(
            ref('str')->stringStyle($style),
            ['str' => $value],
            schema(str_schema('str', nullable: true)),
        ));
    }
}
