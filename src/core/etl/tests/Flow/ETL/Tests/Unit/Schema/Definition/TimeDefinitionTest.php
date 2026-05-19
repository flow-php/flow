<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_entry;
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
        yield 'with date produces datetime' => [
            time_schema('col'),
            date_schema('col'),
            DateTimeDefinition::class,
        ];

        yield 'with datetime produces datetime' => [
            time_schema('col'),
            datetime_schema('col'),
            DateTimeDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = time_schema('duration');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = time_schema('duration');

        static::assertFalse($def->matches(time_entry('other', '10:30:00')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = time_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
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
        $def = time_schema('duration', false, Metadata::with('key', 'value1'));
        $other = time_schema('duration', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = time_schema('duration', true);
        $other = time_schema('duration', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = time_schema('duration');
        $other = string_schema('duration');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = time_schema('duration', true, Metadata::with('key', 'value'));
        $other = time_schema('duration', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = time_schema('duration', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = time_schema('duration');

        static::assertTrue($def->matches(time_entry('duration', '10:30:00')));
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

    public function test_merge_when_both_are_from_null(): void
    {
        $def1 = time_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = time_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        static::assertInstanceOf(TimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null(): void
    {
        $nullDef = time_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = time_schema('col', false);

        $merged = $nullDef->merge($def);

        static::assertInstanceOf(TimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type(): void
    {
        $def = time_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

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

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = time_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = time_schema('duration', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('duration', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = time_schema('col', true);

        static::assertTrue($def->matches(time_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = time_schema('duration');

        $renamed = $def->rename('new_duration');

        static::assertSame('new_duration', $renamed->entry()->name());
        static::assertSame('duration', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = time_schema('duration');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_time_type(): void
    {
        $def = time_schema('duration');

        static::assertSame('time', $def->type()->toString());
    }
}
