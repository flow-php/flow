<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\FloatDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;

final class FloatDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            float_schema('amount'),
            float_schema('amount'),
            true,
        ];

        yield 'same type different name' => [
            float_schema('amount'),
            float_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            float_schema('amount', false),
            float_schema('amount', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            float_schema('amount', true),
            float_schema('amount', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            float_schema('amount'),
            float_schema('amount'),
            float_schema('amount'),
        ];

        yield 'makes nullable when other is nullable' => [
            float_schema('amount', false),
            float_schema('amount', true),
            float_schema('amount', true),
        ];

        yield 'with string produces string' => [
            float_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with integer produces float' => [
            float_schema('col'),
            int_schema('col'),
            FloatDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = float_schema('amount');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(float_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(float_schema('col')->matches(1));
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
        static::assertFalse(float_schema('amount', false, Metadata::with('key', 'value1'))->isSame(float_schema(
            'amount',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(float_schema('amount', true)->isSame(float_schema('amount', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(float_schema('amount')->isSame(string_schema('amount')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(float_schema('amount', true, Metadata::with('key', 'value'))->isSame(float_schema(
            'amount',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = float_schema('amount', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(float_schema('amount')->matches(1.5));
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

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param class-string $expectedClass
     */
    #[DataProvider('provideMergeWithExpectedTypeCases')]
    public function test_merge_produces_expected_type(
        Definition $definition,
        Definition $other,
        string $expectedClass,
    ): void {
        static::assertInstanceOf($expectedClass, $definition->merge($other));
    }

    public function test_merge_with_null_definition_keeps_original_type(): void
    {
        $merged = float_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(FloatDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(float_schema('col', false));

        static::assertInstanceOf(FloatDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = float_schema('amount');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, amount and other');

        $def->merge(float_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', float_schema('col')->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = float_schema('amount', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('amount', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(float_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(float_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(float_schema('col', true)->matches(1.5));
    }

    public function test_rename(): void
    {
        $def = float_schema('amount');

        static::assertSame('new_amount', $def->rename('new_amount')->name());
        static::assertSame('amount', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = float_schema('amount');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_float_type(): void
    {
        static::assertSame('float', float_schema('amount')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', float_schema('col')->merge(bool_schema('col'))->type()->toString());
    }
}
