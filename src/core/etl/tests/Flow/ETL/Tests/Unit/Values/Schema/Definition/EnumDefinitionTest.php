<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\EnumDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use UnitEnum;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\null_schema;
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
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(enum_schema('col', BackedStringEnum::class)->matches(null));
    }

    public function test_does_not_match_a_string_entry_holding_the_enum_class_name(): void
    {
        static::assertFalse(enum_schema('col', BackedStringEnum::class)->matches(BackedStringEnum::class));
    }

    public function test_does_not_match_an_entry_holding_a_different_enum(): void
    {
        static::assertFalse(enum_schema('col', BackedStringEnum::class)->matches(BackedIntEnum::one));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(enum_schema('col', BackedStringEnum::class)->matches(1));
    }

    public function test_enum_class_accessor(): void
    {
        static::assertSame(BackedStringEnum::class, enum_schema('status', BackedStringEnum::class)->enumClass());
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
        static::assertFalse(enum_schema(
            'status',
            BackedStringEnum::class,
            false,
            Metadata::with('key', 'value1'),
        )->isSame(enum_schema('status', BackedStringEnum::class, false, Metadata::with('key', 'value2'))));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(enum_schema('status', BackedStringEnum::class, true)->isSame(enum_schema(
            'status',
            BackedStringEnum::class,
            false,
        )));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(enum_schema('status', BackedStringEnum::class)->isSame(string_schema('status')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(enum_schema(
            'status',
            BackedStringEnum::class,
            true,
            Metadata::with('key', 'value'),
        )->isSame(enum_schema('status', BackedStringEnum::class, true, Metadata::with('key', 'value'))));
    }

    public function test_make_nullable(): void
    {
        $def = enum_schema('status', BackedStringEnum::class, false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_an_entry_holding_any_enum_when_declared_as_unit_enum(): void
    {
        static::assertTrue(enum_schema('col', UnitEnum::class)->matches(BackedStringEnum::one));
    }

    public function test_matches_and_is_compatible_agree_on_a_different_enum(): void
    {
        $def = enum_schema('col', BackedStringEnum::class);

        static::assertFalse($def->matches(BackedIntEnum::one));
        static::assertFalse($def->isCompatible(enum_schema('col', BackedIntEnum::class)));
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(enum_schema('status', BackedStringEnum::class)->matches(BackedStringEnum::one));
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
        $merged = enum_schema('col', BackedStringEnum::class, false)->merge(null_schema('col'));

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(enum_schema('col', BackedStringEnum::class, false));

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_unit_enum_class_adopts_concrete_enum_class(): void
    {
        $merged = enum_schema('col', BackedStringEnum::class)->merge(enum_schema('col', UnitEnum::class, true));

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertSame(BackedStringEnum::class, $merged->enumClass());
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_unit_enum_class_adopts_concrete_enum_class(): void
    {
        $merged = enum_schema('col', UnitEnum::class, true)->merge(enum_schema('col', BackedStringEnum::class));

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertSame(BackedStringEnum::class, $merged->enumClass());
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_of_two_unit_enum_classes_stays_unit_enum(): void
    {
        $merged = enum_schema('col', UnitEnum::class, true)->merge(enum_schema('col', UnitEnum::class, true));

        static::assertInstanceOf(EnumDefinition::class, $merged);
        static::assertSame(UnitEnum::class, $merged->enumClass());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, status and other');

        $def->merge(enum_schema('other', BackedStringEnum::class));
    }

    public function test_merge_with_different_enum_class_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            enum_schema('col', BackedStringEnum::class)
                ->merge(enum_schema('col', BasicEnum::class))
                ->type()
                ->toString(),
        );
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame(
            'string',
            enum_schema('col', BackedStringEnum::class)->merge(new BooleanDefinition('col'))->type()->toString(),
        );
    }

    public function test_normalize(): void
    {
        $normalized = enum_schema('status', BackedStringEnum::class, true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('status', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(enum_schema('col', BackedStringEnum::class, true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(enum_schema('col', BackedStringEnum::class, true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(enum_schema('col', BackedStringEnum::class, true)->matches(BackedStringEnum::one));
    }

    public function test_rename(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);

        static::assertSame('new_status', $def->rename('new_status')->name());
        static::assertSame('status', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = enum_schema('status', BackedStringEnum::class);
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_throws_exception_for_non_existing_enum_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @mago-ignore analysis:possibly-invalid-argument
        enum_schema('status', 'NonExistingEnum');
    }

    public function test_type_returns_enum_type(): void
    {
        static::assertStringContainsString('enum', enum_schema('status', BackedStringEnum::class)->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame(
            'string',
            enum_schema('col', BackedStringEnum::class)->merge(bool_schema('col'))->type()->toString(),
        );
    }
}
