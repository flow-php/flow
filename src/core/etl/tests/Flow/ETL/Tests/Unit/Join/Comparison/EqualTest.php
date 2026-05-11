<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\row;

final class EqualTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Equal('datetime', 'datetime'))->compare(
            row(datetime_entry('datetime', new \DateTimeImmutable('2022-10-01 00:00:00'))),
            row(datetime_entry('datetime', new \DateTimeImmutable('2022-10-01 01:00:00'))),
        ));
    }

    public function test_success(): void
    {
        static::assertTrue((new Equal('datetime', 'datetime'))->compare(
            row(datetime_entry('datetime', $datetime = new \DateTimeImmutable('2022-10-01 00:00:00'))),
            row(datetime_entry('datetime', $datetime)),
        ));
    }
}
