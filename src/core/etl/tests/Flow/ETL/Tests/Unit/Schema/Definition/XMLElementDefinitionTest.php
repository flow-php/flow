<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_element_schema;

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

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = xml_element_schema('element');

        static::assertFalse($def->matches(xml_element_entry('other', '<item>value</item>')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = xml_element_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(XMLElementEntry::class, xml_element_schema('element')->entryClass());
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
        $def = xml_element_schema('element', false, Metadata::with('key', 'value1'));
        $other = xml_element_schema('element', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = xml_element_schema('element', true);
        $other = xml_element_schema('element', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = xml_element_schema('element');
        $other = string_schema('element');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = xml_element_schema('element', true, Metadata::with('key', 'value'));
        $other = xml_element_schema('element', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = xml_element_schema('element', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = xml_element_schema('element');

        static::assertTrue($def->matches(xml_element_entry('element', '<item>value</item>')));
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

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = xml_element_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = xml_element_schema('element', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('element', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = xml_element_schema('col', true);

        static::assertTrue($def->matches(xml_element_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = xml_element_schema('element');

        $renamed = $def->rename('new_element');

        static::assertSame('new_element', $renamed->entry()->name());
        static::assertSame('element', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = xml_element_schema('element');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_xml_element_type(): void
    {
        $def = xml_element_schema('element');

        static::assertSame('xml_element', $def->type()->toString());
    }
}
