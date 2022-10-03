<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Not;
use Flow\ETL\Row;

final class NotTest extends TestCase
{
    public function test_failure() : void
    {
        $this->assertFalse(
            (new Not(new Equal('datetime', 'datetime')))->compare(
                Row::create(Entry::datetime('datetime', $datetime = new \DateTimeImmutable('2022-10-01 00:00:00'))),
                Row::create(Entry::datetime('datetime', $datetime)),
            )
        );
    }

    public function test_success() : void
    {
        $this->assertTrue(
            (new Not(new Equal('datetime', 'datetime')))->compare(
                Row::create(Entry::datetime('datetime', new \DateTimeImmutable('2022-10-01 00:00:00'))),
                Row::create(Entry::datetime('datetime', new \DateTimeImmutable('2022-10-01 01:00:00'))),
            )
        );
    }
}
