<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{date_entry, datetime_entry, lit, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class ModifyDateTimeTest extends FlowTestCase
{
    public function test_modify_date() : void
    {
        self::assertEquals(
            new \DateTimeImmutable('2025-01-01 12:00:00 +00:00'),
            ref('datetime')->modifyDateTime('noon')->eval(
                row(
                    date_entry('datetime', '2025-01-01'),
                )
            )
        );
    }

    public function test_modify_datetime() : void
    {
        self::assertEquals(
            new \DateTimeImmutable('2025-01-01 00:00:00 +00:00'),
            ref('datetime')->modifyDateTime('midnight')->eval(
                row(
                    datetime_entry('datetime', '2025-01-01 10:00:23 +00:00'),
                )
            )
        );
    }

    public function test_modify_using_invalid_modifier() : void
    {
        self::assertNull(
            ref('datetime')->modifyDateTime(lit(1))->eval(
                row(
                    datetime_entry('datetime', '2025-01-01 10:00:23 +00:00'),
                )
            )
        );
    }
}
