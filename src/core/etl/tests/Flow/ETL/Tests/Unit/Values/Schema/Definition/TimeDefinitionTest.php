<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use DateInterval;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\TimeDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_schema;

final class TimeDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            time_schema('duration'),
            time_schema('duration'),
            true,
        ];

        yield 'same type different name' => [
            time_schema('duration'),
            time_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            time_schema('duration', false),
            time_schema('duration', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            time_schema('duration', true),
            time_schema('duration', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            time_schema('duration'),
            time_schema('duration'),
            time_schema('duration'),
        ];

        yield 'makes nullable when other is nullable' => [
            time_schema('duration', false),
            time_schema('duration', true),
            time_schema('duration', true),
        ];

        yield 'with string produces string' => [
            time_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with date produces string' => [
            time_schema('col'),
            date_schema('col'),
            StringDefinition::class,
        ];

        yield 'with datetime produces string' => [
            time_schema('col'),
            datetime_schema('col'),
            StringDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = time_schema('duration');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(time_schema('col')->matches(null));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(time_schema('col')->matches(1));
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
        static::assertFalse(time_schema('duration', false, Metadata::with('key', 'value1'))->isSame(time_schema(
            'duration',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(time_schema('duration', true)->isSame(time_schema('duration', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(time_schema('duration')->isSame(string_schema('duration')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(time_schema('duration', true, Metadata::with('key', 'value'))->isSame(time_schema(
            'duration',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = time_schema('duration', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(time_schema('duration')->matches(new DateInterval('PT10H30M')));
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
        $merged = time_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(TimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(time_schema('col', false));

        static::assertInstanceOf(TimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = time_schema('duration');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, duration and other');

        $def->merge(time_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', time_schema('col')->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = time_schema('duration', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('duration', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(time_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(time_schema('col', true)->matches(null));
    }

    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(time_schema('col', true)->matches(new DateInterval('PT10H30M')));
    }

    public function test_rename(): void
    {
        $def = time_schema('duration');

        static::assertSame('new_duration', $def->rename('new_duration')->name());
        static::assertSame('duration', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = time_schema('duration');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_time_type(): void
    {
        static::assertSame('time', time_schema('duration')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', time_schema('col')->merge(bool_schema('col'))->type()->toString());
    }
}
