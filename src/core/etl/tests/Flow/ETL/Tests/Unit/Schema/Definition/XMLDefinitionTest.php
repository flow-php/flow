<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\xml_entry;
use function Flow\ETL\DSL\xml_schema;

final class XMLDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
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

    public static function provideMergeCases(): Generator
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

    public function test_add_metadata(): void
    {
        $def = xml_schema('document');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = xml_schema('document');

        static::assertFalse($def->matches(xml_entry('other', '<?xml version="1.0"?><root></root>')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = xml_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(XMLEntry::class, xml_schema('document')->entryClass());
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
        $def = xml_schema('document', false, Metadata::with('key', 'value1'));
        $other = xml_schema('document', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = xml_schema('document', true);
        $other = xml_schema('document', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = xml_schema('document');
        $other = string_schema('document');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = xml_schema('document', true, Metadata::with('key', 'value'));
        $other = xml_schema('document', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = xml_schema('document', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = xml_schema('document');

        static::assertTrue($def->matches(xml_entry('document', '<?xml version="1.0"?><root></root>')));
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
        $merged = xml_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(XMLDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(xml_schema('col', false));

        static::assertInstanceOf(XMLDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = xml_schema('document');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, document and other');

        $def->merge(xml_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = xml_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = xml_schema('document', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('document', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = xml_schema('col', true);

        static::assertTrue($def->matches(xml_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = xml_schema('document');

        $renamed = $def->rename('new_document');

        static::assertSame('new_document', $renamed->entry()->name());
        static::assertSame('document', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = xml_schema('document');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_xml_type(): void
    {
        $def = xml_schema('document');

        static::assertSame('xml', $def->type()->toString());
    }
}
