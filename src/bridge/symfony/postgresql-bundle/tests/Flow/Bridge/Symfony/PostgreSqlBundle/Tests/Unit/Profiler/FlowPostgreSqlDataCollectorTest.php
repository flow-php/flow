<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\PostgreSql\Client\Debug\QueryLog;
use Flow\PostgreSql\Client\Debug\RecordedQuery;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(FlowPostgreSqlDataCollector::class)]
final class FlowPostgreSqlDataCollectorTest extends TestCase
{
    public function test_get_name_is_flow_postgresql(): void
    {
        static::assertSame('flow_postgresql', $this->collector(new QueryLog())->getName());
    }

    public function test_late_collect_maps_recorded_queries(): void
    {
        $log = new QueryLog();
        $log->add(
            new RecordedQuery(
                'SELECT * FROM users WHERE id = $1',
                [42],
                1.5,
                1,
                false,
                null,
                'default',
                '/app/Repo.php:10',
            ),
        );
        $collector = $this->collector($log);

        $collector->lateCollect();

        static::assertSame(1, $collector->getQueryCount());
        $query = $collector->getQueries()['default'][0];
        static::assertSame('SELECT * FROM users WHERE id = $1', $query['statement']);
        static::assertSame([42], $query['parameters']);
        static::assertSame(1, $query['returnedRows']);
        static::assertSame(1.5, $query['durationMs']);
        static::assertFalse($query['failed']);
        static::assertNull($query['error']);
        static::assertSame('default', $query['connection']);
        static::assertSame('/app/Repo.php:10', $query['caller']);
        static::assertTrue($query['explainable']);
        static::assertFalse($query['isDuplicate']);
    }

    public function test_late_collect_counts_and_totals(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT 1', [], 2.0, 1, false, null));
        $log->add(new RecordedQuery('SELECT 2', [], 3.0, 1, false, null));
        $log->add(new RecordedQuery('BAD SQL', [], 0.5, null, true, 'syntax error'));
        $collector = $this->collector($log);

        $collector->lateCollect();

        static::assertSame(3, $collector->getQueryCount());
        static::assertSame(1, $collector->getFailedCount());
        static::assertSame(5.5, $collector->getTotalDurationMs());
    }

    public function test_groups_queries_by_connection(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null, 'default'));
        $log->add(new RecordedQuery('SELECT 2', [], 1.0, 1, false, null, 'analytics'));
        $log->add(new RecordedQuery('SELECT 3', [], 1.0, 1, false, null, 'analytics'));
        $collector = $this->collector($log);

        $collector->lateCollect();

        static::assertSame(['default', 'analytics'], $collector->getConnections());
        static::assertCount(1, $collector->getQueries()['default']);
        static::assertCount(2, $collector->getQueries()['analytics']);
    }

    public function test_detects_duplicate_statements(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [1], 1.0, 1, false, null));
        $log->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [2], 1.0, 1, false, null));
        $log->add(new RecordedQuery('SELECT * FROM posts', [], 1.0, 5, false, null));
        $collector = $this->collector($log);

        $collector->lateCollect();

        static::assertSame(1, $collector->getDuplicateCount());
        $rows = $collector->getQueries()['default'];
        static::assertSame(2, $rows[0]['runCount']);
        static::assertTrue($rows[0]['isDuplicate']);
        static::assertSame(1, $rows[2]['runCount']);
        static::assertFalse($rows[2]['isDuplicate']);
    }

    /**
     * @param non-empty-string $statement
     */
    #[TestWith(['SELECT 1', true])]
    #[TestWith(['  with cte as (select 1) select * from cte', true])]
    #[TestWith(['INSERT INTO t (a) VALUES (1)', true])]
    #[TestWith(['UPDATE t SET a = 1', true])]
    #[TestWith(['DELETE FROM t', true])]
    #[TestWith(['SET search_path TO public', false])]
    #[TestWith(['SHOW server_version', false])]
    #[TestWith(['BEGIN', false])]
    #[TestWith(['CREATE TABLE t (a int)', false])]
    public function test_explainable_flag(string $statement, bool $expected): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery($statement, [], 1.0, 1, false, null));
        $collector = $this->collector($log);

        $collector->lateCollect();

        static::assertSame($expected, $collector->getQueries()['default'][0]['explainable']);
    }

    public function test_failed_query_is_not_explainable(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT 1', [], 0.5, null, true, 'boom'));
        $collector = $this->collector($log);

        $collector->lateCollect();

        $query = $collector->getQueries()['default'][0];
        static::assertTrue($query['failed']);
        static::assertSame('boom', $query['error']);
        static::assertFalse($query['explainable']);
    }

    public function test_include_parameters_false_omits_parameters(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [42], 1.0, 1, false, null));
        $collector = $this->collector($log, includeParameters: false);

        $collector->lateCollect();

        static::assertSame([], $collector->getQueries()['default'][0]['parameters']);
    }

    public function test_reset_clears_data_and_query_log(): void
    {
        $log = new QueryLog();
        $log->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null));
        $collector = $this->collector($log);
        $collector->lateCollect();

        $collector->reset();

        static::assertSame([], $collector->getQueries());
        static::assertSame(0, $collector->getQueryCount());
        static::assertSame([], $log->queries());
    }

    private function collector(QueryLog $log, bool $includeParameters = true): FlowPostgreSqlDataCollector
    {
        return new FlowPostgreSqlDataCollector($log, $includeParameters);
    }
}
