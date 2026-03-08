<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, string_schema, xml_entry, xml_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, XMLDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class XMLDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
    {
        yield 'same type and name' => [
            xml_schema('document'),
            xml_schema('document'),
            true,
        ];

        yield 'same type different name' => [
            xml_schema('document'),
            xml_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            xml_schema('document', false),
            xml_schema('document', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            xml_schema('document', true),
            xml_schema('document', false),
            true,
        ];
    }

    public static function provideMergeCases() : \Generator
    {
        yield 'same type' => [
            xml_schema('document'),
            xml_schema('document'),
            xml_schema('document'),
        ];

        yield 'makes nullable when other is nullable' => [
            xml_schema('document', false),
            xml_schema('document', true),
            xml_schema('document', true),
        ];

        yield 'with string produces string' => [
            xml_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata() : void
    {
        $def = xml_schema('document');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = xml_schema('document');

        self::assertFalse($def->matches(xml_entry('other', '<?xml version="1.0"?><root></root>')));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = xml_schema('col');

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
        $def = xml_schema('document', false, Metadata::with('key', 'value1'));
        $other = xml_schema('document', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = xml_schema('document', true);
        $other = xml_schema('document', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = xml_schema('document');
        $other = string_schema('document');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = xml_schema('document', true, Metadata::with('key', 'value'));
        $other = xml_schema('document', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = xml_schema('document', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = xml_schema('document');

        self::assertTrue($def->matches(xml_entry('document', '<?xml version="1.0"?><root></root>')));
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
        $def1 = xml_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = xml_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(XMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = xml_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = xml_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(XMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = xml_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(XMLDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = xml_schema('document');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, document and other');

        $def->merge(xml_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = xml_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = xml_schema('document', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('document', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = xml_schema('col', true);

        self::assertTrue($def->matches(xml_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = xml_schema('document');

        $renamed = $def->rename('new_document');

        self::assertSame('new_document', $renamed->entry()->name());
        self::assertSame('document', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = xml_schema('document');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_xml_type() : void
    {
        $def = xml_schema('document');

        self::assertSame('xml', $def->type()->toString());
    }
}
