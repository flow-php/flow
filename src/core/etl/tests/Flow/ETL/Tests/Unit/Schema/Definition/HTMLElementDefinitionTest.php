<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\HTMLElementEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\html_element_entry;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class HTMLElementDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            html_element_schema('element'),
            html_element_schema('element'),
            true,
        ];

        yield 'same type different name' => [
            html_element_schema('element'),
            html_element_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            html_element_schema('element', false),
            html_element_schema('element', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            html_element_schema('element', true),
            html_element_schema('element', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            html_element_schema('element'),
            html_element_schema('element'),
            html_element_schema('element'),
        ];

        yield 'makes nullable when other is nullable' => [
            html_element_schema('element', false),
            html_element_schema('element', true),
            html_element_schema('element', true),
        ];

        yield 'with string produces string' => [
            html_element_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = html_element_schema('element');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        $def = html_element_schema('col');

        static::assertFalse($def->matches(html_element_entry('col', null)));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = html_element_schema('element');

        static::assertFalse($def->matches(html_element_entry('other', '<div>content</div>')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = html_element_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(HTMLElementEntry::class, html_element_schema('element')->entryClass());
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provideIsCompatibleCases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected): void
    {
        static::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_same_with_different_metadata(): void
    {
        $def = html_element_schema('element', false, Metadata::with('key', 'value1'));
        $other = html_element_schema('element', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = html_element_schema('element', true);
        $other = html_element_schema('element', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = html_element_schema('element');
        $other = string_schema('element');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = html_element_schema('element', true, Metadata::with('key', 'value'));
        $other = html_element_schema('element', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = html_element_schema('element', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = html_element_schema('element');

        static::assertTrue($def->matches(html_element_entry('element', '<div>content</div>')));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param Definition<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merge(Definition $definition, Definition $other, Definition $expected): void
    {
        static::assertEquals($expected, $definition->merge($other));
    }

    public function test_merge_with_null_definition_keeps_original_type(): void
    {
        $merged = html_element_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(HTMLElementDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(html_element_schema('col', false));

        static::assertInstanceOf(HTMLElementDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = html_element_schema('element');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, element and other');

        $def->merge(html_element_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        $def = html_element_schema('col');

        static::assertSame('string', $def->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $def = html_element_schema('element', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('element', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        $def = html_element_schema('col', true);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_nullable_matches_a_null_entry_with_same_name(): void
    {
        $def = html_element_schema('col', true);

        static::assertTrue($def->matches(html_element_entry('col', null)));
    }

    public function test_nullable_matches_a_null_value_carried_by_an_entry_of_a_different_type(): void
    {
        $def = html_element_schema('col', true);

        static::assertTrue($def->matches(int_entry('col', null)));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        $def = html_element_schema('col', true);

        static::assertTrue($def->matches(html_element_entry('col', '<div>content</div>')));
    }

    public function test_rename(): void
    {
        $def = html_element_schema('element');

        $renamed = $def->rename('new_element');

        static::assertSame('new_element', $renamed->entry()->name());
        static::assertSame('element', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = html_element_schema('element');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_html_element_type(): void
    {
        $def = html_element_schema('element');

        static::assertSame('html_element', $def->type()->toString());
    }

    public function test_merge_with_union_containing_this_type_returns_union(): void
    {
        $merged = html_element_schema('col')->merge(
            new UnionDefinition('col', type_union(type_html_element(), type_boolean())),
        );

        static::assertInstanceOf(UnionDefinition::class, $merged);
        static::assertSame('boolean|html_element', $merged->type()->toString());
    }

    public function test_merge_with_union_not_containing_this_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            html_element_schema('col')
                ->merge(new UnionDefinition('col', type_union(type_integer(), type_string())))
                ->type()
                ->toString(),
        );
    }
}
