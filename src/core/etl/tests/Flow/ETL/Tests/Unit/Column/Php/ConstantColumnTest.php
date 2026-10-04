<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Column\Php\ListColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\Physical\IdentityPhysical;
use Flow\ETL\Column\Physical\NullPhysical;
use Flow\ETL\Column\Physical\TimeZonePhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function array_map;
use function bin2hex;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function str_repeat;

final class ConstantColumnTest extends TestCase
{
    public function test_one_value_for_every_row(): void
    {
        $column = new ConstantColumn(type_time_zone(), new TimeZonePhysical(), 'UTC', 3);

        static::assertSame(3, $column->count());
        static::assertSame('UTC', $column->at(2));
        static::assertSame(['UTC', 'UTC', 'UTC'], $column->physicals());
        static::assertEquals(new DateTimeZone('UTC'), $column->value(1));
        static::assertEquals(
            [new DateTimeZone('UTC'), new DateTimeZone('UTC'), new DateTimeZone('UTC')],
            $column->values(),
        );
        static::assertFalse($column->isNull(0));
        static::assertSame(0, $column->nullCount());
        static::assertSame(2, $column->slice(1, 2)->count());
        static::assertSame(4, $column->take([0, 0, 1, 2])->count());
        static::assertEquals(type_time_zone(), $column->type());
    }

    public function test_zero_rows(): void
    {
        $column = new ConstantColumn(type_integer(), new IdentityPhysical(), 1, 0);

        static::assertSame([], $column->physicals());
        static::assertSame([], $column->values());
    }

    public function test_null(): void
    {
        $column = new ConstantColumn(type_optional(type_integer()), new NullPhysical(), null, 2);

        static::assertTrue($column->isNull(0));
        static::assertSame(2, $column->nullCount());
        static::assertNull($column->value(0));
        static::assertSame(['00', str_repeat('0', 32)], array_map(bin2hex(...), $column->encode()));
    }

    public function test_expands_to_the_column_kind(): void
    {
        $column = new ConstantColumn(type_list(type_integer()), new IdentityPhysical(), [1, 2], 2);
        $expanded = $column->expand();

        static::assertInstanceOf(ListColumn::class, $expanded);
        static::assertSame([[1, 2], [1, 2]], $expanded->physicals());
    }

    public function test_the_null_kind_does_not_expand_and_has_no_buffers(): void
    {
        foreach ([type_null(), type_optional(type_null())] as $type) {
            $column = new ConstantColumn($type, new NullPhysical(), null, 2);

            static::assertTrue($column->isNullKind());
            static::assertSame($column, $column->expand());
            static::assertSame([], $column->encode());
        }
    }

    public function test_concat_with_a_scalar_column_expands(): void
    {
        $constant = new ConstantColumn(type_integer(), new IdentityPhysical(), 7, 1);

        static::assertSame([7, 1, 2], $constant->concat(ColumnMother::of(int_schema('a'), [1, 2]))->physicals());
        static::assertSame(
            [7, 8, 8],
            $constant->concat(new ConstantColumn(type_integer(), new IdentityPhysical(), 8, 2))->physicals(),
        );
    }

    public function test_with_type_restamps_the_value(): void
    {
        $column = (new PhpBackend())->constant(
            datetime_schema('a'),
            new DateTimeImmutable('2024-01-02 03:04:05 UTC'),
            2,
        );
        $warsaw = type_instance_of(DateTimeImmutable::class)->assert(
            $column->withType(type_datetime('Europe/Warsaw'))->value(1),
        );

        static::assertSame('2024-01-02 04:04:05 Europe/Warsaw', $warsaw->format('Y-m-d H:i:s e'));
    }

    public function test_with_type_refuses_another_column_kind(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('integer and string are different column kinds');

        (new PhpBackend())
            ->constant(int_schema('a'), 1, 2)
            ->withType(type_string());
    }
}
