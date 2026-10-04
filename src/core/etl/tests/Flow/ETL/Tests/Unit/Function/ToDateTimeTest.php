<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use stdClass;

use function array_slice;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\random_string;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_date_time;
use function Flow\Types\DSL\type_datetime;

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
            (new FunctionContext(flow_context()))->eval(
                to_date_time(ref('date_time')),
                ['date_time' => new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC'))],
                schema(datetime_schema('date_time')),
            ),
        );
    }

    public function test_int_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            (new FunctionContext(flow_context()))->eval(
                to_date_time(ref('int')),
                [
                    'int' => (int) (new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')))->format('U'),
                ],
                schema(int_schema('int')),
            ),
        );
    }

    public function test_string_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            (new FunctionContext(flow_context()))->eval(
                to_date_time(ref('string'), 'Y-m-d H:i:s'),
                [
                    'string' => '2020-01-01 00:00:00',
                ],
                schema(str_schema('string')),
            ),
        );
    }

    public function test_unparseable_string_to_date_time_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            to_date_time(ref('string'), 'Y-m-d H:i:s'),
            [
                'string' => 'not a datetime',
            ],
            schema(str_schema('string')),
        ));
    }

    public function test_constant_zone_returns_a_zoned_type(): void
    {
        static::assertSame(
            '?datetime<Europe/Warsaw>',
            to_date_time(ref('at'), 'Y-m-d H:i:s', new DateTimeZone('Europe/Warsaw'))->returns()->toString(),
        );
    }

    public function test_object_keeps_its_time(): void
    {
        static::assertSame(
            '10:30:00',
            type_datetime()
                ->assert((new FunctionContext(flow_context()))->eval(
                    to_date_time(ref('at')),
                    ['at' => new DateTimeImmutable('2020-01-01 10:30:00', new DateTimeZone('UTC'))],
                    schema(datetime_schema('at')),
                ))
                ->format('H:i:s'),
        );
    }

    public function test_int_lands_in_the_constant_zone(): void
    {
        static::assertSame(
            '2020-01-01 11:30:00 Europe/Warsaw',
            type_datetime()
                ->assert((new FunctionContext(flow_context()))->eval(
                    to_date_time(ref('at'), 'Y-m-d H:i:s', new DateTimeZone('Europe/Warsaw')),
                    [
                        'at' => 1577874600,
                    ],
                    schema(int_schema('at')),
                ))
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_mutable_datetime_is_converted_without_mutation(): void
    {
        $value = new DateTime('2020-01-01 10:30:00', new DateTimeZone('UTC'));

        static::assertEquals(
            new DateTimeImmutable('2020-01-01 11:30:00', new DateTimeZone('Europe/Warsaw')),
            (new FunctionContext(flow_context()))->eval(
                to_date_time(ref('at'), 'Y-m-d H:i:s', new DateTimeZone('Europe/Warsaw')),
                [
                    'at' => $value,
                ],
                schema(datetime_schema('at')),
            ),
        );
        static::assertSame('2020-01-01 10:30:00 UTC', $value->format('Y-m-d H:i:s e'));
    }

    public function test_non_datetime_object_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ToDateTime function requires DateTimeInterface object');

        (new FunctionContext(flow_context()))->eval(to_date_time(lit(new stdClass())), [], schema());
    }

    public function test_string_in_a_utc_alias_zone_lands_in_utc(): void
    {
        static::assertSame(
            '2020-01-01 10:30:00 UTC',
            type_datetime()
                ->assert((new FunctionContext(flow_context()))->eval(
                    to_date_time(ref('at'), 'Y-m-d H:i:s', new DateTimeZone('GMT')),
                    [
                        'at' => '2020-01-01 10:30:00',
                    ],
                    schema(str_schema('at')),
                ))
                ->format('Y-m-d H:i:s e'),
        );
    }
}
