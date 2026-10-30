<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\Types\DSL\type_datetime;

final class DateTimeDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            datetime_schema('created_at'),
            datetime_schema('created_at'),
            true,
        ];

        yield 'same type different name' => [
            datetime_schema('created_at'),
            datetime_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            datetime_schema('created_at', false),
            datetime_schema('created_at', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            datetime_schema('created_at', true),
            datetime_schema('created_at', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            datetime_schema('created_at'),
            datetime_schema('created_at'),
            datetime_schema('created_at'),
        ];

        yield 'makes nullable when other is nullable' => [
            datetime_schema('created_at', false),
            datetime_schema('created_at', true),
            datetime_schema('created_at', true),
        ];

        yield 'with string produces string' => [
            datetime_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];

        yield 'same zone keeps the zone' => [
            datetime_schema('col', zone: 'Europe/Warsaw'),
            datetime_schema('col', zone: 'Europe/Warsaw'),
            datetime_schema('col', zone: 'Europe/Warsaw'),
        ];

        yield 'different zones widen to utc' => [
            datetime_schema('col', zone: 'Europe/Warsaw'),
            datetime_schema('col'),
            datetime_schema('col'),
        ];

        yield 'with date keeps the zone' => [
            datetime_schema('col', zone: 'Europe/Warsaw'),
            date_schema('col'),
            datetime_schema('col', zone: 'Europe/Warsaw'),
        ];

        yield 'with null keeps the zone' => [
            datetime_schema('col', zone: 'Europe/Warsaw'),
            null_schema('col'),
            datetime_schema('col', true, zone: 'Europe/Warsaw'),
        ];

        yield 'with time produces string' => [
            datetime_schema('col', zone: 'Europe/Warsaw'),
            time_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with date produces datetime' => [
            datetime_schema('col'),
            date_schema('col'),
            DateTimeDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = datetime_schema('created_at');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(datetime_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(datetime_schema('col')->matches(1));
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
        static::assertFalse(datetime_schema(
            'created_at',
            false,
            Metadata::with('key', 'value1'),
        )->isSame(datetime_schema('created_at', false, Metadata::with('key', 'value2'))));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(datetime_schema('created_at', true)->isSame(datetime_schema('created_at', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(datetime_schema('created_at')->isSame(string_schema('created_at')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(datetime_schema('created_at', true, Metadata::with('key', 'value'))->isSame(datetime_schema(
            'created_at',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = datetime_schema('created_at', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(datetime_schema('created_at')->matches(type_datetime()->cast('2024-01-15 10:30:00')));
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
        $merged = datetime_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(DateTimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(datetime_schema('col', false));

        static::assertInstanceOf(DateTimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = datetime_schema('created_at');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, created_at and other');

        $def->merge(datetime_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', datetime_schema('col')->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = datetime_schema('created_at', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('created_at', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertSame(['type' => 'datetime', 'zone' => 'UTC'], $normalized['type']);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(datetime_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(datetime_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(datetime_schema('col', true)->matches(type_datetime()->cast('2024-01-15 10:30:00')));
    }

    public function test_rename(): void
    {
        $def = datetime_schema('created_at');

        static::assertSame('updated_at', $def->rename('updated_at')->name());
        static::assertSame('created_at', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = datetime_schema('created_at');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_datetime_type(): void
    {
        static::assertSame('datetime', datetime_schema('created_at')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', datetime_schema('col')->merge(bool_schema('col'))->type()->toString());
    }

    public function test_is_compatible_ignores_the_zone(): void
    {
        static::assertTrue(datetime_schema('at', zone: 'Europe/Warsaw')->isCompatible(datetime_schema('at')));
    }

    public function test_is_same_compares_the_zone(): void
    {
        static::assertFalse(datetime_schema('at', zone: 'Europe/Warsaw')->isSame(datetime_schema('at')));
        static::assertTrue(datetime_schema('at', zone: 'Europe/Warsaw')->isSame(datetime_schema(
            'at',
            zone: 'Europe/Warsaw',
        )));
    }

    public function test_rename_keeps_the_zone(): void
    {
        static::assertSame(
            'datetime<Europe/Warsaw>',
            datetime_schema('at', zone: 'Europe/Warsaw')->rename('b')->type()->toString(),
        );
    }

    public function test_nullability_and_metadata_changes_keep_the_zone(): void
    {
        $definition = datetime_schema('at', zone: 'Europe/Warsaw');

        static::assertSame('datetime<Europe/Warsaw>', $definition->makeNullable()->type()->toString());
        static::assertSame('datetime<Europe/Warsaw>', $definition->addMetadata('k', 'v')->type()->toString());
        static::assertSame(
            'datetime<Europe/Warsaw>',
            $definition->setMetadata(Metadata::with('k', 'v'))->type()->toString(),
        );
    }
}
