<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\NullType;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\null_schema;

final class NullDefinitionTest extends FlowTestCase
{
    public function test_add_metadata(): void
    {
        $withMeta = null_schema('id')->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
    }

    public function test_entry(): void
    {
        static::assertSame('id', null_schema('id')->entry()->name());
    }

    public function test_entry_class_is_null_entry(): void
    {
        static::assertSame(NullEntry::class, null_schema('id')->entryClass());
    }

    public function test_is_always_nullable(): void
    {
        static::assertTrue(null_schema('id')->isNullable());
        static::assertTrue(null_schema('id')->makeNullable(false)->isNullable());
    }

    public function test_is_compatible_with_another_null_definition(): void
    {
        static::assertTrue(null_schema('id')->isCompatible(null_schema('id')));
    }

    public function test_is_not_compatible_with_a_different_name(): void
    {
        static::assertFalse(null_schema('id')->isCompatible(null_schema('other')));
    }

    public function test_is_not_compatible_with_a_different_type(): void
    {
        static::assertFalse(null_schema('id')->isCompatible(int_schema('id')));
    }

    public function test_is_not_same_when_metadata_differs(): void
    {
        static::assertFalse(null_schema('id', Metadata::with('k', 'v1'))->isSame(null_schema('id', Metadata::with(
            'k',
            'v2',
        ))));
    }

    public function test_is_not_same_when_the_other_is_not_nullable(): void
    {
        static::assertFalse(null_schema('id')->isSame(int_schema('id', false)));
    }

    public function test_is_same_with_an_identical_definition(): void
    {
        static::assertTrue(null_schema('id', Metadata::with('k', 'v'))->isSame(null_schema('id', Metadata::with(
            'k',
            'v',
        ))));
    }

    public function test_make_nullable_returns_a_nullable_definition(): void
    {
        static::assertTrue(null_schema('id')->makeNullable()->isNullable());
    }

    public function test_matches_a_null_entry_with_the_same_name(): void
    {
        static::assertTrue(null_schema('id')->matches(null_entry('id')));
    }

    public function test_matches_any_entry_with_the_same_name(): void
    {
        static::assertTrue(null_schema('id')->matches(int_entry('id', 1)));
    }

    public function test_does_not_match_an_entry_with_a_different_name(): void
    {
        static::assertFalse(null_schema('id')->matches(null_entry('other')));
    }

    public function test_merge_of_two_null_definitions_stays_a_null_definition(): void
    {
        $merged = null_schema('id')->merge(null_schema('id'));

        static::assertInstanceOf(NullDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_of_two_null_definitions_merges_metadata(): void
    {
        $merged = null_schema('id', Metadata::with('a', 1))->merge(null_schema('id', Metadata::with('b', 2)));

        static::assertTrue($merged->metadata()->has('a'));
        static::assertTrue($merged->metadata()->has('b'));
    }

    public function test_merge_with_a_typed_definition_produces_that_type_made_nullable(): void
    {
        $merged = null_schema('id')->merge(int_schema('id', false));

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_a_typed_definition_merges_metadata(): void
    {
        $merged = null_schema('id', Metadata::with('a', 1))->merge(int_schema('id', false, Metadata::with('b', 2)));

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->metadata()->has('a'));
        static::assertTrue($merged->metadata()->has('b'));
    }

    public function test_typed_definition_merged_with_null_definition_becomes_nullable(): void
    {
        $merged = int_schema('id', false)->merge(null_schema('id'));

        static::assertInstanceOf(IntegerDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_a_different_name_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, id and other');

        null_schema('id')->merge(null_schema('other'));
    }

    public function test_normalize(): void
    {
        static::assertSame(
            [
                'ref' => 'id',
                'type' => ['type' => 'null'],
                'nullable' => true,
                'metadata' => ['k' => 'v'],
            ],
            null_schema('id', Metadata::with('k', 'v'))->normalize(),
        );
    }

    public function test_rename(): void
    {
        $renamed = null_schema('id')->rename('new_id');

        static::assertSame('new_id', $renamed->entry()->name());
        static::assertInstanceOf(NullDefinition::class, $renamed);
    }

    public function test_set_metadata(): void
    {
        $metadata = Metadata::with('key', 'value');

        static::assertTrue(null_schema('id')->setMetadata($metadata)->metadata()->isEqual($metadata));
    }

    public function test_type_is_null_type(): void
    {
        static::assertInstanceOf(NullType::class, null_schema('id')->type());
    }
}
