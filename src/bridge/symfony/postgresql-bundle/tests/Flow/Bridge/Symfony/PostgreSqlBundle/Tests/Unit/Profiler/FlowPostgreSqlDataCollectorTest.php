<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorder;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorderOptions;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\RecordedQuery;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

#[CoversClass(FlowPostgreSqlDataCollector::class)]
final class FlowPostgreSqlDataCollectorTest extends TestCase
{
    public function test_get_name_is_flow_postgresql(): void
    {
        static::assertSame('flow_postgresql', $this->collector(new QueryRecorder())->getName());
    }

    public function test_late_collect_maps_recorded_queries(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(
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
        $collector = $this->collector($recorder);

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
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT 1', [], 2.0, 1, false, null));
        $recorder->add(new RecordedQuery('SELECT 2', [], 3.0, 1, false, null));
        $recorder->add(new RecordedQuery('BAD SQL', [], 0.5, null, true, 'syntax error'));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertSame(3, $collector->getQueryCount());
        static::assertSame(1, $collector->getFailedCount());
        static::assertSame(5.5, $collector->getTotalDurationMs());
    }

    public function test_groups_queries_by_connection(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null, 'default'));
        $recorder->add(new RecordedQuery('SELECT 2', [], 1.0, 1, false, null, 'analytics'));
        $recorder->add(new RecordedQuery('SELECT 3', [], 1.0, 1, false, null, 'analytics'));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertSame(['default', 'analytics'], $collector->getConnections());
        static::assertCount(1, $collector->getQueries()['default']);
        static::assertCount(2, $collector->getQueries()['analytics']);
    }

    public function test_detects_duplicate_statements(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [1], 1.0, 1, false, null));
        $recorder->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [2], 1.0, 1, false, null));
        $recorder->add(new RecordedQuery('SELECT * FROM posts', [], 1.0, 5, false, null));
        $collector = $this->collector($recorder);

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
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery($statement, [], 1.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertSame($expected, $collector->getQueries()['default'][0]['explainable']);
    }

    public function test_failed_query_is_not_explainable(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT 1', [], 0.5, null, true, 'boom'));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        $query = $collector->getQueries()['default'][0];
        static::assertTrue($query['failed']);
        static::assertSame('boom', $query['error']);
        static::assertFalse($query['explainable']);
    }

    public function test_include_parameters_false_omits_parameters(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(includeParameters: false));
        $recorder->add(new RecordedQuery('SELECT * FROM users WHERE id = $1', [42], 1.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        $query = $collector->getQueries()['default'][0];
        static::assertSame([], $query['parameters']);
        static::assertFalse($query['explainable']);
    }

    public function test_totals_are_accurate_after_eviction(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 1));
        $recorder->add(new RecordedQuery('SELECT 1', [], 2.0, 1, false, null));
        $recorder->add(new RecordedQuery('BAD SQL', [], 0.5, null, true, 'syntax error'));
        $recorder->add(new RecordedQuery('SELECT 2', [], 3.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertSame(3, $collector->getQueryCount());
        static::assertSame(1, $collector->getRetainedCount());
        static::assertSame(1, $collector->getFailedCount());
        static::assertSame(5.5, $collector->getTotalDurationMs());
        static::assertCount(1, $collector->getQueries()['default']);
    }

    public function test_query_with_dropped_parameters_is_not_explainable(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 1));
        $recorder->add(new RecordedQuery('SELECT * FROM t WHERE a = $1 AND b = $2', [1, 2], 1.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertFalse($collector->getQueries()['default'][0]['explainable']);
    }

    public function test_query_without_any_parameters_stays_explainable(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(includeParameters: false));
        $recorder->add(new RecordedQuery('SELECT NOW()', [], 1.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        static::assertTrue($collector->getQueries()['default'][0]['explainable']);
    }

    public function test_parameters_truncated_is_exposed_on_the_row(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 1));
        $recorder->add(new RecordedQuery('SELECT $1, $2', [1, 2], 1.0, 1, false, null));
        $recorder->add(new RecordedQuery('SELECT $1', [1], 1.0, 1, false, null));
        $collector = $this->collector($recorder);

        $collector->lateCollect();

        $rows = $collector->getQueries()['default'];
        static::assertTrue($rows[0]['parametersTruncated']);
        static::assertFalse($rows[1]['parametersTruncated']);
    }

    public function test_reset_clears_data_and_query_log(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null));
        $collector = $this->collector($recorder);
        $collector->lateCollect();

        $collector->reset();

        static::assertSame([], $collector->getQueries());
        static::assertSame(0, $collector->getQueryCount());
        static::assertSame([], $recorder->queries());
    }

    private function collector(QueryRecorder $recorder): FlowPostgreSqlDataCollector
    {
        return new FlowPostgreSqlDataCollector($recorder);
    }
}
