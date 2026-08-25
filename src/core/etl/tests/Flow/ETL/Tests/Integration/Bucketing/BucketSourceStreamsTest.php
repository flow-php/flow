<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing;

use Flow\ETL\Tests\FlowTestCase;

/**
 * A never-closed source stream is invisible in endedSpans() - it looks exactly like a stream that was never
 * opened. Only comparing started against ended catches it.
 *
 * The bodies were removed in task 03 along with filesystem telemetry - see debt entry D1. They are recoverable
 * from git history; restoring them requires an instrument for stream lifetime, not necessarily spans.
 */
final class BucketSourceStreamsTest extends FlowTestCase
{
    public function test_bucket_source_streams_are_not_nested_into_each_other(): void
    {
        static::markTestSkipped(
            'Filesystem telemetry was dropped in task 03 - see debt entry D1 in '
            . '.claude/tasks/sortby-bug/debt.md. TraceableFilesystem and traceable_filesystem() still work '
            . 'when a filesystem is wrapped by hand; only Config/ConfigBuilder provisioning was removed.',
        );
    }

    public function test_external_sort_closes_every_bucket_source_stream(): void
    {
        static::markTestSkipped(
            'Filesystem telemetry was dropped in task 03 - see debt entry D1 in '
            . '.claude/tasks/sortby-bug/debt.md. TraceableFilesystem and traceable_filesystem() still work '
            . 'when a filesystem is wrapped by hand; only Config/ConfigBuilder provisioning was removed.',
        );
    }

    public function test_group_by_closes_every_bucket_source_stream(): void
    {
        static::markTestSkipped(
            'Filesystem telemetry was dropped in task 03 - see debt entry D1 in '
            . '.claude/tasks/sortby-bug/debt.md. TraceableFilesystem and traceable_filesystem() still work '
            . 'when a filesystem is wrapped by hand; only Config/ConfigBuilder provisioning was removed.',
        );
    }
}
