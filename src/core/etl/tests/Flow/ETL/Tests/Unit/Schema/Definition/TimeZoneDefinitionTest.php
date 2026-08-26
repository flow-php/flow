<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\TimeZoneEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\TimeZoneType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\time_zone_entry;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_union;

final class TimeZoneDefinitionTest extends FlowTestCase
{
    public static function provide_is_compatible_cases(): Generator
    {
        yield 'same type and name' => [time_zone_schema('tz'), time_zone_schema('tz'), true];
        yield 'same type different name' => [time_zone_schema('tz'), time_zone_schema('other'), false];
        yield 'not nullable with nullable' => [time_zone_schema('tz', false), time_zone_schema('tz', true), false];
        yield 'nullable with not nullable' => [time_zone_schema('tz', true), time_zone_schema('tz', false), true];
        yield 'different type' => [time_zone_schema('tz'), string_schema('tz'), false];
    }

    public function test_add_metadata(): void
    {
        $definition = time_zone_schema('tz');

        static::assertSame('value', $definition->addMetadata('key', 'value')->metadata()->get('key'));
        static::assertFalse($definition->metadata()->has('key'));
    }

    public function test_entry(): void
    {
        static::assertSame('tz', time_zone_schema('tz')->entry()->name());
    }

    public function test_entry_class(): void
    {
        static::assertSame(TimeZoneEntry::class, time_zone_schema('tz')->entryClass());
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provide_is_compatible_cases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected): void
    {
        static::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_nullable(): void
    {
        static::assertFalse(time_zone_schema('tz')->isNullable());
        static::assertTrue(time_zone_schema('tz', true)->isNullable());
    }

    public function test_is_same(): void
    {
        static::assertTrue(time_zone_schema('tz')->isSame(time_zone_schema('tz')));
        static::assertFalse(time_zone_schema('tz')->isSame(time_zone_schema('tz', true)));
        static::assertFalse(time_zone_schema('tz')->isSame(string_schema('tz')));
        static::assertFalse(time_zone_schema('tz', false, Metadata::with('k', 'v1'))->isSame(time_zone_schema(
            'tz',
            false,
            Metadata::with('k', 'v2'),
        )));
    }

    public function test_make_nullable(): void
    {
        static::assertTrue(time_zone_schema('tz')->makeNullable()->isNullable());
        static::assertFalse(time_zone_schema('tz', true)->makeNullable(false)->isNullable());
    }

    public function test_matches(): void
    {
        static::assertTrue(time_zone_schema('tz')->matches(time_zone_entry('tz', 'UTC')));
        static::assertFalse(time_zone_schema('tz')->matches(time_zone_entry('other', 'UTC')));
        static::assertFalse(time_zone_schema('tz')->matches(int_entry('tz', 1)));
        static::assertFalse(time_zone_schema('tz')->matches(time_zone_entry('tz', null)));
        static::assertTrue(time_zone_schema('tz', true)->matches(time_zone_entry('tz', null)));
    }

    public function test_merge_with_itself_keeps_the_type(): void
    {
        static::assertInstanceOf(TimeZoneDefinition::class, time_zone_schema('tz')->merge(time_zone_schema('tz')));
        static::assertTrue(time_zone_schema('tz')->merge(time_zone_schema('tz', true))->isNullable());
    }

    public function test_merge_with_null_definition_makes_it_nullable(): void
    {
        $merged = time_zone_schema('tz')->merge(null_schema('tz'));

        static::assertInstanceOf(TimeZoneDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_string_produces_string(): void
    {
        static::assertInstanceOf(StringDefinition::class, time_zone_schema('tz')->merge(string_schema('tz')));
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame(
            'string',
            time_zone_schema('tz')
                ->merge(new UnionDefinition('tz', type_union(type_boolean(), type_string())))
                ->type()
                ->toString(),
        );
    }

    public function test_merge_with_a_union_containing_this_type_returns_a_union(): void
    {
        static::assertInstanceOf(
            UnionDefinition::class,
            time_zone_schema('tz')->merge(new UnionDefinition('tz', type_union(type_time_zone(), type_boolean()))),
        );
    }

    public function test_merge_with_a_different_name_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, tz and other');

        time_zone_schema('tz')->merge(time_zone_schema('other'));
    }

    public function test_merges_metadata_from_both_sides(): void
    {
        $merged = time_zone_schema('tz', false, Metadata::with('a', 1))->merge(time_zone_schema(
            'tz',
            false,
            Metadata::with('b', 2),
        ));

        static::assertTrue($merged->metadata()->has('a'));
        static::assertTrue($merged->metadata()->has('b'));
    }

    public function test_normalize(): void
    {
        static::assertSame(
            [
                'ref' => 'tz',
                'type' => ['type' => 'timezone'],
                'nullable' => true,
                'metadata' => ['k' => 'v'],
            ],
            time_zone_schema('tz', true, Metadata::with('k', 'v'))->normalize(),
        );
    }

    public function test_rename(): void
    {
        static::assertSame('other', time_zone_schema('tz')->rename('other')->entry()->name());
    }

    public function test_set_metadata(): void
    {
        static::assertSame('v', time_zone_schema('tz')->setMetadata(Metadata::with('k', 'v'))->metadata()->get('k'));
    }

    public function test_type(): void
    {
        static::assertInstanceOf(TimeZoneType::class, time_zone_schema('tz')->type());
    }

    public function test_does_not_match_a_string_entry_carrying_a_timezone_name(): void
    {
        static::assertFalse(time_zone_schema('tz')->matches(str_entry('tz', 'UTC')));
    }
}
