<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{html_entry, html_schema, int_entry, string_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, HTMLDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\{DataProvider, RequiresPhp};

final class HTMLDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
    {
        yield 'same type and name' => [
            html_schema('content'),
            html_schema('content'),
            true,
        ];

        yield 'same type different name' => [
            html_schema('content'),
            html_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            html_schema('content', false),
            html_schema('content', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            html_schema('content', true),
            html_schema('content', false),
            true,
        ];
    }

    public static function provideMergeCases() : \Generator
    {
        yield 'same type' => [
            html_schema('content'),
            html_schema('content'),
            html_schema('content'),
        ];

        yield 'makes nullable when other is nullable' => [
            html_schema('content', false),
            html_schema('content', true),
            html_schema('content', true),
        ];

        yield 'with string produces string' => [
            html_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata() : void
    {
        $def = html_schema('content');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    #[RequiresPhp('>= 8.4')]
    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = html_schema('content');

        self::assertFalse($def->matches(html_entry('other', '<html><body></body></html>')));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = html_schema('col');

        self::assertFalse($def->matches(int_entry('col', 1)));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provideIsCompatibleCases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected) : void
    {
        self::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_same_with_different_metadata() : void
    {
        $def = html_schema('content', false, Metadata::with('key', 'value1'));
        $other = html_schema('content', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = html_schema('content', true);
        $other = html_schema('content', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = html_schema('content');
        $other = string_schema('content');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = html_schema('content', true, Metadata::with('key', 'value'));
        $other = html_schema('content', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = html_schema('content', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    #[RequiresPhp('>= 8.4')]
    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = html_schema('content');

        self::assertTrue($def->matches(html_entry('content', '<html><body></body></html>')));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param Definition<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merge(Definition $definition, Definition $other, Definition $expected) : void
    {
        self::assertEquals($expected, $definition->merge($other));
    }

    public function test_merge_when_both_are_from_null() : void
    {
        $def1 = html_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = html_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(HTMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = html_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = html_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(HTMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = html_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(HTMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = html_schema('content');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, content and other');

        $def->merge(html_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = html_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = html_schema('content', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('content', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_alias_for_make_nullable() : void
    {
        $def = html_schema('content', false);

        $nullable = $def->nullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = html_schema('col', true);

        self::assertTrue($def->matches(html_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = html_schema('content');

        $renamed = $def->rename('new_content');

        self::assertSame('new_content', $renamed->entry()->name());
        self::assertSame('content', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = html_schema('content');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_html_type() : void
    {
        $def = html_schema('content');

        self::assertSame('html', $def->type()->toString());
    }
}
