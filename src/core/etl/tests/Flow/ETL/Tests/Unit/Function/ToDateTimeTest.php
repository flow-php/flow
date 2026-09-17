<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\FlowTestCase;

use function array_slice;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\random_string;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\to_date_time;

final class ToDateTimeTest extends FlowTestCase
{
    public function test_the_default_format_is_deterministic(): void
    {
        static::assertTrue(to_date_time(ref('value'))->deterministic());
    }

    public function test_a_format_that_leaves_the_time_out_is_not_deterministic(): void
    {
        static::assertFalse(to_date_time(ref('value'), 'Y-m-d')->deterministic());
    }

    public function test_a_format_that_resets_unparsed_fields_is_deterministic(): void
    {
        static::assertTrue(to_date_time(ref('value'), '!Y-m-d')->deterministic());
    }

    public function test_a_format_known_only_at_run_time_is_not_deterministic(): void
    {
        static::assertFalse(to_date_time(ref('value'), ref('format'))->deterministic());
    }

    public function test_a_non_deterministic_child_makes_it_non_deterministic(): void
    {
        static::assertFalse(to_date_time(random_string(10))->deterministic());
    }

    public function test_a_rebuild_keeping_the_format_child_keeps_the_answer(): void
    {
        $function = to_date_time(ref('value'));

        static::assertTrue(
            $function->withChildren([ref('renamed'), ...array_slice($function->children(), 1)])->deterministic(),
        );
    }

    public function test_a_rebuild_with_another_format_child_does_not_know_the_format(): void
    {
        $function = to_date_time(ref('value'));
        $children = $function->children();

        static::assertFalse($function->withChildren([$children[0], lit('Y-m-d H:i:s'), $children[2]])->deterministic());
    }

    public function test_date_time_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date_time(ref('date_time'))
                ->eval(row(['date_time' => new DateTimeImmutable(
                    '2020-01-01 00:00:00',
                    new DateTimeZone('UTC'),
                )]), flow_context()),
        );
    }

    public function test_int_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date_time(ref('int'))
                ->eval(row([
                    'int' => (int) (new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')))->format('U'),
                ]), flow_context()),
        );
    }

    public function test_string_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date_time(ref('string'), 'Y-m-d H:i:s')->eval(row(['string' => '2020-01-01 00:00:00']), flow_context()),
        );
    }

    public function test_unparseable_string_to_date_time_is_null(): void
    {
        static::assertNull(to_date_time(ref('string'), 'Y-m-d H:i:s')->eval(row([
            'string' => 'not a datetime',
        ]), flow_context()));
    }
}
