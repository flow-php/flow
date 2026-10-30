<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\Types\DSL\type_xml_element;

final class XMLElementDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            xml_element_schema('element'),
            xml_element_schema('element'),
            true,
        ];

        yield 'same type different name' => [
            xml_element_schema('element'),
            xml_element_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            xml_element_schema('element', false),
            xml_element_schema('element', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            xml_element_schema('element', true),
            xml_element_schema('element', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            xml_element_schema('element'),
            xml_element_schema('element'),
            xml_element_schema('element'),
        ];

        yield 'makes nullable when other is nullable' => [
            xml_element_schema('element', false),
            xml_element_schema('element', true),
            xml_element_schema('element', true),
        ];

        yield 'with string produces string' => [
            xml_element_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = xml_element_schema('element');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(xml_element_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(xml_element_schema('col')->matches(1));
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
        static::assertFalse(xml_element_schema(
            'element',
            false,
            Metadata::with('key', 'value1'),
        )->isSame(xml_element_schema('element', false, Metadata::with('key', 'value2'))));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(xml_element_schema('element', true)->isSame(xml_element_schema('element', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(xml_element_schema('element')->isSame(string_schema('element')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(xml_element_schema(
            'element',
            true,
            Metadata::with('key', 'value'),
        )->isSame(xml_element_schema('element', true, Metadata::with('key', 'value'))));
    }

    public function test_make_nullable(): void
    {
        $def = xml_element_schema('element', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(xml_element_schema('element')->matches(type_xml_element()->cast('<item>value</item>')));
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
        $merged = xml_element_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(XMLElementDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(xml_element_schema('col', false));

        static::assertInstanceOf(XMLElementDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = xml_element_schema('element');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, element and other');

        $def->merge(xml_element_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            xml_element_schema('col')->merge(new BooleanDefinition('col'))->type()->toString(),
        );
    }

    public function test_normalize(): void
    {
        $normalized = xml_element_schema('element', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('element', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(xml_element_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(xml_element_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(xml_element_schema('col', true)->matches(type_xml_element()->cast('<item>value</item>')));
    }

    public function test_rename(): void
    {
        $def = xml_element_schema('element');

        static::assertSame('new_element', $def->rename('new_element')->name());
        static::assertSame('element', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = xml_element_schema('element');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_xml_element_type(): void
    {
        static::assertSame('xml_element', xml_element_schema('element')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', xml_element_schema('col')->merge(bool_schema('col'))->type()->toString());
    }
}
