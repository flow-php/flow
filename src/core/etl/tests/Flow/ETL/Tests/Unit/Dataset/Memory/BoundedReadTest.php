<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Memory;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Dataset\Memory\BoundedRead;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\FlowTestCase;

final class BoundedReadTest extends FlowTestCase
{
    public function test_a_stream_within_the_limit_is_read_whole(): void
    {
        [$read, $fits] = (new BoundedRead(Unit::fromGb(64), new PhpBackend()))->read(IdRows::batches([1, 2], [3]));

        static::assertTrue($fits);
        static::assertSame([[1, 2], [3]], array_map(static fn($rows): array => $rows->reduceToArray('id'), $read));
    }

    public function test_past_the_limit_reading_stops_and_the_rest_continues_where_it_stopped(): void
    {
        $rows = IdRows::batches([1, 2], [3], [4]);
        $bounded = new BoundedRead(Unit::fromBytes(1), new PhpBackend());

        [$read, $fits] = $bounded->read($rows);

        static::assertFalse($fits);
        static::assertCount(1, $read);
        static::assertSame([1, 2, 3, 4], IdRows::ids($bounded->followedBy($read, $rows)));
    }

    public function test_followed_by_a_finished_stream_yields_only_what_was_read(): void
    {
        $rows = IdRows::batches([1]);
        $bounded = new BoundedRead(Unit::fromGb(64), new PhpBackend());

        [$read] = $bounded->read($rows);

        static::assertSame([1], IdRows::ids($bounded->followedBy($read, $rows)));
    }
}
