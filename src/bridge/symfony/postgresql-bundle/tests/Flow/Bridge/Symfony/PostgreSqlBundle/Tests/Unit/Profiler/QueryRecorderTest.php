<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorder;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorderOptions;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\RecordedQuery;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_fill;
use function gc_collect_cycles;
use function memory_get_usage;

#[CoversClass(QueryRecorder::class)]
#[CoversClass(RecordedQuery::class)]
final class QueryRecorderTest extends TestCase
{
    public function test_starts_empty(): void
    {
        static::assertSame([], (new QueryRecorder())->queries());
    }

    public function test_add_appends_in_order(): void
    {
        $recorder = new QueryRecorder();
        $first = new RecordedQuery('SELECT 1', [], 0.5, 1, false, null);
        $second = new RecordedQuery('SELECT 2', [7], 1.5, 0, false, null);

        $recorder->add($first);
        $recorder->add($second);

        static::assertSame([$first, $second], $recorder->queries());
    }

    public function test_reset_clears_entries(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('SELECT 1', [], 0.5, 1, false, null));

        $recorder->reset();

        static::assertSame([], $recorder->queries());
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
        static::assertFalse($query->parametersTruncated);
    }

    public function test_evicts_oldest_entry_when_cap_reached(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 2));
        $first = new RecordedQuery('SELECT 1', [], 1.0, 1, false, null);
        $second = new RecordedQuery('SELECT 2', [], 1.0, 1, false, null);
        $third = new RecordedQuery('SELECT 3', [], 1.0, 1, false, null);

        $recorder->add($first);
        $recorder->add($second);
        $recorder->add($third);

        static::assertSame([$second, $third], $recorder->queries());
    }

    public function test_counters_survive_eviction(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 1));

        for ($i = 0; $i < 5; $i++) {
            $recorder->add(new RecordedQuery('SELECT ' . $i, [], 1.0, 1, false, null));
        }

        static::assertSame(5, $recorder->recordedCount());
        static::assertSame(1, $recorder->retainedCount());
    }

    public function test_failed_count_survives_eviction(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 1));
        $recorder->add(new RecordedQuery('BAD 1', [], 1.0, null, true, 'boom'));
        $recorder->add(new RecordedQuery('BAD 2', [], 1.0, null, true, 'boom'));
        $recorder->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null));

        static::assertSame(2, $recorder->failedCount());
        static::assertSame(1, $recorder->retainedCount());
    }

    public function test_total_duration_survives_eviction(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 1));
        $recorder->add(new RecordedQuery('SELECT 1', [], 2.0, 1, false, null));
        $recorder->add(new RecordedQuery('SELECT 2', [], 3.5, 1, false, null));

        static::assertSame(5.5, $recorder->totalDurationMs());
    }

    public function test_reset_clears_entries_and_counters(): void
    {
        $recorder = new QueryRecorder();
        $recorder->add(new RecordedQuery('BAD', [], 2.0, null, true, 'boom'));

        $recorder->reset();

        static::assertSame([], $recorder->queries());
        static::assertSame(0, $recorder->recordedCount());
        static::assertSame(0, $recorder->retainedCount());
        static::assertSame(0, $recorder->failedCount());
        static::assertSame(0.0, $recorder->totalDurationMs());
    }

    public function test_parameters_dropped_when_over_max_parameters(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 2));
        $recorder->add(new RecordedQuery('INSERT INTO t VALUES ($1, $2, $3)', [1, 2, 3], 1.0, 3, false, null));

        static::assertSame([], $recorder->queries()[0]->parameters);
        static::assertTrue($recorder->queries()[0]->parametersTruncated);
    }

    public function test_parameters_kept_when_within_max_parameters(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 2));
        $query = new RecordedQuery('SELECT * FROM t WHERE a = $1 AND b = $2', [1, 2], 1.0, 1, false, null);

        $recorder->add($query);

        static::assertSame($query, $recorder->queries()[0]);
        static::assertFalse($recorder->queries()[0]->parametersTruncated);
    }

    public function test_parameters_dropped_when_include_parameters_disabled(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(includeParameters: false));
        $recorder->add(new RecordedQuery('SELECT * FROM t WHERE id = $1', [42], 1.0, 1, false, null));

        static::assertSame([], $recorder->queries()[0]->parameters);
        static::assertTrue($recorder->queries()[0]->parametersTruncated);
    }

    public function test_parameterless_query_is_not_marked_truncated(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(includeParameters: false));
        $recorder->add(new RecordedQuery('SELECT NOW()', [], 1.0, 1, false, null));

        static::assertFalse($recorder->queries()[0]->parametersTruncated);
    }

    public function test_dropped_entry_keeps_every_other_field(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 0));
        $recorder->add(
            new RecordedQuery(
                'INSERT INTO t VALUES ($1)',
                [42],
                12.5,
                7,
                true,
                'boom',
                'analytics',
                '/app/Repo.php:10',
            ),
        );

        $dropped = $recorder->queries()[0];

        static::assertSame('INSERT INTO t VALUES ($1)', $dropped->sql);
        static::assertSame(12.5, $dropped->durationMs);
        static::assertSame(7, $dropped->rowCount);
        static::assertTrue($dropped->failed);
        static::assertSame('boom', $dropped->error);
        static::assertSame('analytics', $dropped->connection);
        static::assertSame('/app/Repo.php:10', $dropped->caller);
    }

    public function test_memory_is_flat_across_many_batched_inserts(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 100, maxRetainedParameters: 10));

        for ($i = 0; $i < 500; $i++) {
            $recorder->add(
                new RecordedQuery('INSERT INTO t VALUES ($1)', array_fill(0, 2_000, 'value'), 1.0, 1, false, null),
            );
        }

        gc_collect_cycles();
        $afterFirstHalf = memory_get_usage();

        for ($i = 0; $i < 500; $i++) {
            $recorder->add(
                new RecordedQuery('INSERT INTO t VALUES ($1)', array_fill(0, 2_000, 'value'), 1.0, 1, false, null),
            );
        }

        gc_collect_cycles();

        static::assertSame(100, $recorder->retainedCount());
        static::assertSame(1000, $recorder->recordedCount());
        static::assertLessThan(512 * 1024, memory_get_usage() - $afterFirstHalf);
    }

    public function test_statement_is_truncated_when_over_max_query_length(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueryLength: 10));

        $recorder->add(new RecordedQuery('SELECT * FROM a_very_long_table_name', [], 1.0, 1, false, null));

        static::assertSame('SELECT * F...', $recorder->queries()[0]->sql);
        static::assertTrue($recorder->queries()[0]->statementTruncated);
    }

    public function test_statement_is_kept_when_within_max_query_length(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueryLength: 100));

        $recorder->add(new RecordedQuery('SELECT 1', [], 1.0, 1, false, null));

        static::assertSame('SELECT 1', $recorder->queries()[0]->sql);
        static::assertFalse($recorder->queries()[0]->statementTruncated);
    }

    public function test_statement_is_kept_when_max_query_length_is_null(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueryLength: null));
        $sql = 'SELECT ' . str_repeat('a', 10_000);

        $recorder->add(new RecordedQuery($sql, [], 1.0, 1, false, null));

        static::assertSame($sql, $recorder->queries()[0]->sql);
        static::assertFalse($recorder->queries()[0]->statementTruncated);
    }

    public function test_truncated_statement_keeps_every_other_field(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueryLength: 5));
        $recorder->add(
            new RecordedQuery(
                'INSERT INTO t VALUES ($1)',
                [42],
                12.5,
                7,
                true,
                'boom',
                'analytics',
                '/app/Repo.php:10',
            ),
        );

        $truncated = $recorder->queries()[0];

        static::assertSame([42], $truncated->parameters);
        static::assertSame(12.5, $truncated->durationMs);
        static::assertSame(7, $truncated->rowCount);
        static::assertTrue($truncated->failed);
        static::assertSame('boom', $truncated->error);
        static::assertSame('analytics', $truncated->connection);
        static::assertSame('/app/Repo.php:10', $truncated->caller);
        static::assertFalse($truncated->parametersTruncated);
    }

    public function test_statement_and_parameters_can_both_be_truncated(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxRetainedParameters: 1, maxQueryLength: 5));

        $recorder->add(new RecordedQuery('INSERT INTO t VALUES ($1, $2)', [1, 2], 1.0, 1, false, null));

        static::assertSame('INSER...', $recorder->queries()[0]->sql);
        static::assertSame([], $recorder->queries()[0]->parameters);
        static::assertTrue($recorder->queries()[0]->statementTruncated);
        static::assertTrue($recorder->queries()[0]->parametersTruncated);
    }

    public function test_memory_is_bounded_with_huge_batch_insert_statements(): void
    {
        $recorder = new QueryRecorder(new QueryRecorderOptions(maxQueries: 100, maxRetainedParameters: 10));

        gc_collect_cycles();
        $baseline = memory_get_usage();

        for ($i = 0; $i < 500; $i++) {
            $recorder->add(
                new RecordedQuery('INSERT INTO t VALUES ' . str_repeat("(\$1),", 20_000) . $i, [], 1.0, 1, false, null),
            );
        }

        gc_collect_cycles();

        static::assertSame(100, $recorder->retainedCount());
        static::assertLessThan(512 * 1024, memory_get_usage() - $baseline);
    }
}
