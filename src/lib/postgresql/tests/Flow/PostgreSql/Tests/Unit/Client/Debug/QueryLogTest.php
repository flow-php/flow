<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Debug;

use Flow\PostgreSql\Client\Debug\QueryLog;
use Flow\PostgreSql\Client\Debug\RecordedQuery;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(QueryLog::class)]
#[CoversClass(RecordedQuery::class)]
final class QueryLogTest extends TestCase
{
    public function test_starts_empty(): void
    {
        static::assertSame([], (new QueryLog())->queries());
    }

    public function test_add_appends_in_order(): void
    {
        $log = new QueryLog();
        $first = new RecordedQuery('SELECT 1', [], 0.5, 1, false, null);
        $second = new RecordedQuery('SELECT 2', [7], 1.5, 0, false, null);

        $log->add($first);
        $log->add($second);

        static::assertSame([$first, $second], $log->queries());
    }

    public function test_reset_clears_entries(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT 1', [], 0.5, 1, false, null));

        $log->reset();

        static::assertSame([], $log->queries());
    }

    public function test_recorded_query_exposes_values(): void
    {
        $query = new RecordedQuery('SELECT * FROM t WHERE id = $1', [42], 2.25, 1, true, 'boom');

        static::assertSame('SELECT * FROM t WHERE id = $1', $query->sql);
        static::assertSame([42], $query->parameters);
        static::assertSame(2.25, $query->durationMs);
        static::assertSame(1, $query->rowCount);
        static::assertTrue($query->failed);
        static::assertSame('boom', $query->error);
    }
}
