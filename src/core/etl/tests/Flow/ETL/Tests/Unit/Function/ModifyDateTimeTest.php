<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;

final class ModifyDateTimeTest extends FlowTestCase
{
    public function test_modify_date(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2025-01-01 12:00:00 +00:00'),
            ref('datetime')
                ->modifyDateTime('noon')
                ->eval(row(['datetime' => type_date()->cast('2025-01-01')]), flow_context()),
        );
    }

    public function test_modify_datetime(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2025-01-01 00:00:00 +00:00'),
            ref('datetime')
                ->modifyDateTime('midnight')
                ->eval(row(['datetime' => type_datetime()->cast('2025-01-01 10:00:23 +00:00')]), flow_context()),
        );
    }

    public function test_modify_using_invalid_modifier(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        ref('datetime')
            ->modifyDateTime(lit(1))
            ->eval(row(['datetime' => type_datetime()->cast('2025-01-01 10:00:23 +00:00')]), flow_context());
    }

    public function test_zone_of_the_reference_is_kept(): void
    {
        static::assertSame(
            'datetime<Europe/Warsaw>',
            (new ReferenceResolver())
                ->resolve(ref('at')->modifyDateTime('+1 day'), schema(datetime_schema('at', zone: 'Europe/Warsaw')))
                ->returns()
                ->toString(),
        );
    }

    public function test_zone_of_a_nullable_reference_is_kept(): void
    {
        static::assertSame(
            'datetime<Europe/Warsaw>',
            (new ReferenceResolver())
                ->resolve(
                    ref('at')->modifyDateTime('+1 day'),
                    schema(datetime_schema('at', nullable: true, zone: 'Europe/Warsaw')),
                )
                ->returns()
                ->toString(),
        );
    }

    public function test_non_datetime_reference_returns_utc_datetime(): void
    {
        static::assertSame(
            'datetime',
            (new ReferenceResolver())
                ->resolve(ref('at')->modifyDateTime('+1 day'), schema(str_schema('at')))
                ->returns()
                ->toString(),
        );
    }
}
