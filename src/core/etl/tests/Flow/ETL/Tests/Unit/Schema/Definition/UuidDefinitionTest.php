<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use function Flow\ETL\DSL\{int_entry, string_schema, uuid_entry, uuid_schema};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition\{BooleanDefinition, UuidDefinition};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class UuidDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases() : \Generator
    {
        yield 'same type and name' => [
            uuid_schema('id'),
            uuid_schema('id'),
            true,
        ];

        yield 'same type different name' => [
            uuid_schema('id'),
            uuid_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            uuid_schema('id', false),
            uuid_schema('id', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            uuid_schema('id', true),
            uuid_schema('id', false),
            true,
        ];
    }

    public static function provideMergeCases() : \Generator
    {
        yield 'same type' => [
            uuid_schema('id'),
            uuid_schema('id'),
            uuid_schema('id'),
        ];

        yield 'makes nullable when other is nullable' => [
            uuid_schema('id', false),
            uuid_schema('id', true),
            uuid_schema('id', true),
        ];

        yield 'with string produces string' => [
            uuid_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata() : void
    {
        $def = uuid_schema('id');

        $withMeta = $def->addMetadata('key', 'value');

        self::assertTrue($withMeta->metadata()->has('key'));
        self::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name() : void
    {
        $def = uuid_schema('id');

        self::assertFalse($def->matches(uuid_entry('other', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')));
    }

    public function test_does_not_match_entry_with_different_type() : void
    {
        $def = uuid_schema('col');

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
        $def = uuid_schema('id', false, Metadata::with('key', 'value1'));
        $other = uuid_schema('id', false, Metadata::with('key', 'value2'));

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability() : void
    {
        $def = uuid_schema('id', true);
        $other = uuid_schema('id', false);

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type() : void
    {
        $def = uuid_schema('id');
        $other = string_schema('id');

        self::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition() : void
    {
        $def = uuid_schema('id', true, Metadata::with('key', 'value'));
        $other = uuid_schema('id', true, Metadata::with('key', 'value'));

        self::assertTrue($def->isSame($other));
    }

    public function test_make_nullable() : void
    {
        $def = uuid_schema('id', false);

        $nullable = $def->makeNullable();

        self::assertTrue($nullable->isNullable());
        self::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type() : void
    {
        $def = uuid_schema('id');

        self::assertTrue($def->matches(uuid_entry('id', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')));
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
        $def1 = uuid_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = uuid_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        self::assertInstanceOf(UuidDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null() : void
    {
        $nullDef = uuid_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = uuid_schema('col', false);

        $merged = $nullDef->merge($def);

        self::assertInstanceOf(UuidDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
        self::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type() : void
    {
        $def = uuid_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        self::assertInstanceOf(UuidDefinition::class, $merged);
        self::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception() : void
    {
        $def = uuid_schema('id');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, id and other');

        $def->merge(uuid_schema('other'));
    }

    public function test_merge_with_incompatible_type_throws_exception() : void
    {
        $def = uuid_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize() : void
    {
        $def = uuid_schema('id', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        self::assertSame('id', $normalized['ref']);
        self::assertTrue($normalized['nullable']);
        self::assertArrayHasKey('type', $normalized);
        self::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name() : void
    {
        $def = uuid_schema('col', true);

        self::assertTrue($def->matches(uuid_entry('col', null)));
    }

    public function test_rename() : void
    {
        $def = uuid_schema('id');

        $renamed = $def->rename('new_id');

        self::assertSame('new_id', $renamed->entry()->name());
        self::assertSame('id', $def->entry()->name());
    }

    public function test_set_metadata() : void
    {
        $def = uuid_schema('id');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        self::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_uuid_type() : void
    {
        $def = uuid_schema('id');

        self::assertSame('uuid', $def->type()->toString());
    }
}
