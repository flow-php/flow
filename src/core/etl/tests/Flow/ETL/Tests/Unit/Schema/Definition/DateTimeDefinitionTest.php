<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_schema;

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
    }

    public static function provideMergeWithExpectedTypeCases(): Generator
    {
        yield 'with date produces datetime' => [
            datetime_schema('col'),
            date_schema('col'),
            DateTimeDefinition::class,
        ];

        yield 'with time produces datetime' => [
            datetime_schema('col'),
            time_schema('col'),
            DateTimeDefinition::class,
        ];
    }

    public function test_add_metadata(): void
    {
        $def = datetime_schema('created_at');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_does_not_match_entry_with_different_name(): void
    {
        $def = datetime_schema('created_at');

        static::assertFalse($def->matches(datetime_entry('other', '2024-01-15 10:30:00')));
    }

    public function test_does_not_match_entry_with_different_type(): void
    {
        $def = datetime_schema('col');

        static::assertFalse($def->matches(int_entry('col', 1)));
    }

    public function test_entry_class(): void
    {
        static::assertSame(DateTimeEntry::class, datetime_schema('created_at')->entryClass());
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
        $def = datetime_schema('created_at', false, Metadata::with('key', 'value1'));
        $other = datetime_schema('created_at', false, Metadata::with('key', 'value2'));

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_nullability(): void
    {
        $def = datetime_schema('created_at', true);
        $other = datetime_schema('created_at', false);

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_different_type(): void
    {
        $def = datetime_schema('created_at');
        $other = string_schema('created_at');

        static::assertFalse($def->isSame($other));
    }

    public function test_is_same_with_identical_definition(): void
    {
        $def = datetime_schema('created_at', true, Metadata::with('key', 'value'));
        $other = datetime_schema('created_at', true, Metadata::with('key', 'value'));

        static::assertTrue($def->isSame($other));
    }

    public function test_make_nullable(): void
    {
        $def = datetime_schema('created_at', false);

        $nullable = $def->makeNullable();

        static::assertTrue($nullable->isNullable());
        static::assertFalse($def->isNullable());
    }

    public function test_matches_entry_with_same_name_and_type(): void
    {
        $def = datetime_schema('created_at');

        static::assertTrue($def->matches(datetime_entry('created_at', '2024-01-15 10:30:00')));
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
        $def1 = datetime_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def2 = datetime_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def1->merge($def2);

        static::assertInstanceOf(DateTimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertTrue($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_when_this_is_from_null(): void
    {
        $nullDef = datetime_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));
        $def = datetime_schema('col', false);

        $merged = $nullDef->merge($def);

        static::assertInstanceOf(DateTimeDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
        static::assertFalse($merged->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_merge_with_assumed_null_keeps_original_type(): void
    {
        $def = datetime_schema('col', false);
        $nullDef = string_schema('col', true, Metadata::fromArray([Metadata::FROM_NULL => true]));

        $merged = $def->merge($nullDef);

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

    public function test_merge_with_incompatible_type_throws_exception(): void
    {
        $def = datetime_schema('col');

        $this->expectException(RuntimeException::class);

        $def->merge(new BooleanDefinition('col'));
    }

    public function test_normalize(): void
    {
        $def = datetime_schema('created_at', true, Metadata::with('key', 'value'));

        $normalized = $def->normalize();

        static::assertSame('created_at', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_matches_any_entry_with_same_name(): void
    {
        $def = datetime_schema('col', true);

        static::assertTrue($def->matches(datetime_entry('col', null)));
    }

    public function test_rename(): void
    {
        $def = datetime_schema('created_at');

        $renamed = $def->rename('updated_at');

        static::assertSame('updated_at', $renamed->entry()->name());
        static::assertSame('created_at', $def->entry()->name());
    }

    public function test_set_metadata(): void
    {
        $def = datetime_schema('created_at');
        $metadata = Metadata::with('key', 'value');

        $withMeta = $def->setMetadata($metadata);

        static::assertTrue($withMeta->metadata()->isEqual($metadata));
    }

    public function test_type_returns_datetime_type(): void
    {
        $def = datetime_schema('created_at');

        static::assertSame('datetime', $def->type()->toString());
    }
}
