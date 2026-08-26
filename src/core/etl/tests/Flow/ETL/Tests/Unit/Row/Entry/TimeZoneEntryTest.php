<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\TimeZoneEntry;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\TimeZoneType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\time_zone_entry;

final class TimeZoneEntryTest extends FlowTestCase
{
    public static function provide_is_equal_cases(): Generator
    {
        yield 'same zone' => [time_zone_entry('tz', 'UTC'), time_zone_entry('tz', 'UTC'), true];
        yield 'different zone' => [time_zone_entry('tz', 'UTC'), time_zone_entry('tz', 'Europe/Warsaw'), false];
        yield 'different name' => [time_zone_entry('tz', 'UTC'), time_zone_entry('other', 'UTC'), false];
        yield 'both null' => [time_zone_entry('tz', null), time_zone_entry('tz', null), true];
        yield 'one null' => [time_zone_entry('tz', 'UTC'), time_zone_entry('tz', null), false];
        yield 'not a timezone entry' => [time_zone_entry('tz', 'UTC'), int_entry('tz', 1), false];
    }

    public function test_definition(): void
    {
        static::assertInstanceOf(TimeZoneDefinition::class, time_zone_entry('tz', 'UTC')->definition());
        static::assertFalse(time_zone_entry('tz', 'UTC')->definition()->isNullable());
        static::assertTrue(time_zone_entry('tz', null)->definition()->isNullable());
    }

    public function test_empty_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        new TimeZoneEntry('', new DateTimeZone('UTC'));
    }

    public function test_from_builds_an_entry_from_a_zone_name(): void
    {
        static::assertSame('Europe/Warsaw', TimeZoneEntry::from('tz', 'Europe/Warsaw')->toString());
    }

    public function test_is(): void
    {
        static::assertTrue(time_zone_entry('tz', 'UTC')->is('tz'));
        static::assertTrue(time_zone_entry('tz', 'UTC')->is(ref('tz')));
        static::assertFalse(time_zone_entry('tz', 'UTC')->is('other'));
        static::assertFalse(time_zone_entry('tz', 'UTC')->is(ref('other')));
    }

    /**
     * @param Entry<mixed> $other
     */
    #[DataProvider('provide_is_equal_cases')]
    public function test_is_equal(TimeZoneEntry $entry, Entry $other, bool $expected): void
    {
        static::assertSame($expected, $entry->isEqual($other));
    }

    public function test_name(): void
    {
        static::assertSame('tz', time_zone_entry('tz', 'UTC')->name());
    }

    public function test_rename_preserves_the_value_and_metadata(): void
    {
        $renamed = time_zone_entry('tz', 'UTC', Metadata::with('k', 'v'))->rename('other');

        static::assertSame('other', $renamed->name());
        static::assertSame('UTC', $renamed->toString());
        static::assertSame('v', $renamed->definition()->metadata()->get('k'));
    }

    public function test_to_string_is_the_zone_name(): void
    {
        static::assertSame('Europe/Warsaw', time_zone_entry('tz', 'Europe/Warsaw')->toString());
        static::assertSame('Europe/Warsaw', (string) time_zone_entry('tz', 'Europe/Warsaw'));
        static::assertSame('', time_zone_entry('tz', null)->toString());
    }

    public function test_type(): void
    {
        static::assertInstanceOf(TimeZoneType::class, time_zone_entry('tz', 'UTC')->type());
    }

    public function test_value_returns_the_date_time_zone(): void
    {
        static::assertEquals(new DateTimeZone('UTC'), time_zone_entry('tz', 'UTC')->value());
        static::assertNull(time_zone_entry('tz', null)->value());
    }

    public function test_ref_is_supplied_by_the_shared_trait(): void
    {
        static::assertTrue(time_zone_entry('tz', 'UTC')->ref()->is(ref('tz')));
    }
}
