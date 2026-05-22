<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\string_schema;

final class EnumDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            enum_schema('status', BackedStringEnum::class),
            enum_schema('status', BackedStringEnum::class),
            true,
        ];

        yield 'same type different name' => [
            enum_schema('status', BackedStringEnum::class),
            enum_schema('other', BackedStringEnum::class),
            false,
        ];

        yield 'not nullable with nullable' => [
            enum_schema('status', BackedStringEnum::class, false),
            enum_schema('status', BackedStringEnum::class, true),
            false,
        ];

        yield 'nullable with not nullable' => [
            enum_schema('status', BackedStringEnum::class, true),
            enum_schema('status', BackedStringEnum::class, false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type and enum class' => [
            enum_schema('status', BackedStringEnum::class),
            enum_schema('status', BackedStringEnum::class),
            enum_schema('status', BackedStringEnum::class),
        ];

        yield 'makes nullable when other is nullable' => [
            enum_schema('status', BackedStringEnum::class, false),
            enum_schema('status', BackedStringEnum::class, true),
            enum_schema('status', BackedStringEnum::class, true),
        ];

        yield 'with string produces string' => [
            enum_schema('col', BackedStringEnum::class),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        static::assertFalse($def->matches(enum_entry('other', BackedStringEnum::one)));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = enum_schema('col', BackedStringEnum::class);

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_enum_class_accessor(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        static::assertSame(BackedStringEnum::class, $def->enumClass());
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
        $def = enum_schema('status', BackedStringEnum::class, false, Metadata::with('key', 'value1'));
        $other = enum_schema('status', BackedStringEnum::class, false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = enum_schema('status', BackedStringEnum::class, true);
        $other = enum_schema('status', BackedStringEnum::class, false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);
        $other = string_schema('status');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = enum_schema('status', BackedStringEnum::class, true, Metadata::with('key', 'value'));
        $other = enum_schema('status', BackedStringEnum::class, true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = enum_schema('status', BackedStringEnum::class, false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        static::assertTrue($def->matches(enum_entry('status', BackedStringEnum::one)));
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

    public function test_merge_when_both_are_from_null(): void
    {
        $def1 = enum_schema('col', BackedStringEnum::class, true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = enum_schema('col', BackedStringEnum::class, true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null(): void
    {
        $nullDef = enum_schema('col', BackedStringEnum::class, true, Metadata::fromArray([
            Metadata::FROM_NULL => true,
        ]));
        $def = enum_schema('col', BackedStringEnum::class, false);

        $merged = $nullDef->merge($def);

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type(): void
    {
        $def = enum_schema('col', BackedStringEnum::class, false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, status and other');

        $def->merge(enum_schema('other', BackedStringEnum::class));
    }

    public function test_merge_with_different_enum_class_throws_exception(): void
    {
        $def = enum_schema('col', BackedStringEnum::class);

        $this->expectException(RuntimeException::class);

        $def->merge(enum_schema('col', BasicEnum::class));
    }

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = enum_schema('col', BackedStringEnum::class);

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = enum_schema('status', BackedStringEnum::class, true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('status', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = enum_schema('col', BackedStringEnum::class, true);

        static::assertTrue($def->matches(enum_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        $renamed = $def->rename('new_status');

        static::assertSame('new_status', $renamed->entry()->name());
        static::assertSame('status', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_throws_exception_for_non_existing_enum_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @mago-ignore analysis:possibly-invalid-argument
        /** @phpstan-ignore argument.type, argument.templateType */
        enum_schema('status', 'NonExistingEnum');
    }

    public function test_type_returns_enum_type(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        static::assertStringContainsString('enum', $def->type()->toString());
    }
}
