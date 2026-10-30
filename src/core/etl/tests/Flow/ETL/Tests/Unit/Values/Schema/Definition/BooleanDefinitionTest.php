<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\uuid_schema;

final class BooleanDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            bool_schema('flag'),
            bool_schema('flag'),
            true,
        ];

        yield 'same type different name' => [
            bool_schema('flag'),
            bool_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            bool_schema('flag', false),
            bool_schema('flag', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            bool_schema('flag', true),
            bool_schema('flag', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            bool_schema('flag'),
            bool_schema('flag'),
            bool_schema('flag'),
        ];

        yield 'makes nullable when other is nullable' => [
            bool_schema('flag', false),
            bool_schema('flag', true),
            bool_schema('flag', true),
        ];

        yield 'with string produces string' => [
            bool_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = bool_schema('flag');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(bool_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(bool_schema('col')->matches(1));
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
        static::assertFalse(bool_schema('flag', false, Metadata::with('key', 'value1'))->isSame(bool_schema(
            'flag',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(bool_schema('flag', true)->isSame(bool_schema('flag', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(bool_schema('flag')->isSame(string_schema('flag')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(bool_schema('flag', true, Metadata::with('key', 'value'))->isSame(bool_schema(
            'flag',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = bool_schema('flag', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(bool_schema('flag')->matches(true));
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
        $merged = bool_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(BooleanDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(bool_schema('col', false));

        static::assertInstanceOf(BooleanDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = bool_schema('flag');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, flag and other');

        $def->merge(bool_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', bool_schema('col')->merge(new IntegerDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = bool_schema('flag', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('flag', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(bool_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(bool_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(bool_schema('col', true)->matches(true));
    }

    public function test_rename(): void
    {
        $def = bool_schema('flag');

        static::assertSame('new_name', $def->rename('new_name')->name());
        static::assertSame('flag', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = bool_schema('flag');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_boolean_type(): void
    {
        static::assertSame('boolean', bool_schema('flag')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', bool_schema('col')->merge(uuid_schema('col'))->type()->toString());
    }
}
