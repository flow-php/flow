<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Pipeline\Limiter;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class LimiterTest extends TestCase
{
    public function test_expanding_above_limit() : void
    {
        $limiter = new Limiter(5);

        $limiter->limit(new Rows(
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 2)),
            Row::create(Entry::integer('id', 3))
        ));

        $this->assertFalse($limiter->limitReached());

        $limited = $limiter->limitTransformed(new Rows(
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 2)),
            Row::create(Entry::integer('id', 2)),
            Row::create(Entry::integer('id', 3)),
            Row::create(Entry::integer('id', 3))
        ));

        $this->assertEquals(
            $latest = new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 3)),
            ),
            $limited
        );
        $this->assertEquals(
            $latest,
            $limiter->latest()
        );
        $this->assertTrue($limiter->limitReached());
        $this->assertNull($limiter->limit(new Rows(
            Row::create(Entry::integer('id', 10)),
            Row::create(Entry::integer('id', 11)),
        )));
    }

    public function test_limit() : void
    {
        $limiter = new Limiter(5);

        $limiter->limit(new Rows(
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 2)),
            Row::create(Entry::integer('id', 3))
        ));

        $this->assertFalse($limiter->limitReached());

        $limited = $limiter->limit(new Rows(
            Row::create(Entry::integer('id', 4)),
            Row::create(Entry::integer('id', 5)),
            Row::create(Entry::integer('id', 6))
        ));

        $this->assertEquals(
            $latest = new Rows(
                Row::create(Entry::integer('id', 4)),
                Row::create(Entry::integer('id', 5)),
            ),
            $limited
        );
        $this->assertEquals(
            $latest,
            $limiter->latest()
        );
        $this->assertTrue($limiter->limitReached());

        $this->assertNull($limiter->limit(new Rows(
            Row::create(Entry::integer('id', 10)),
            Row::create(Entry::integer('id', 11)),
        )));
    }

    public function test_limit_below_zero() : void
    {
        $this->expectException(InvalidArgumentException::class);
        new Limiter(-1);
    }

    public function test_limit_zero() : void
    {
        $this->expectException(InvalidArgumentException::class);
        new Limiter(0);
    }

    public function test_passing_through_anything_when_limit_is_null() : void
    {
        $limiter = new Limiter(null);

        $this->assertSame(
            $rows = new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 3))
            ),
            $limiter->limit($rows)
        );

        $this->assertSame(
            $rows = new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 3))
            ),
            $limiter->limitTransformed($rows)
        );
    }

    public function test_reducing_below_limit() : void
    {
        $limiter = new Limiter(5);

        $limiter->limit(new Rows(
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 2)),
            Row::create(Entry::integer('id', 3))
        ));

        $this->assertFalse($limiter->limitReached());

        $this->assertSame(
            $rows = new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2)),
            ),
            $limiter->limitTransformed($rows)
        );
    }
}
