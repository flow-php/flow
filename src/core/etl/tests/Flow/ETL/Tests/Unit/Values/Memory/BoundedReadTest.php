<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Memory\Unit;

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
        [$read, $fits] = (new BoundedRead(Unit::fromBytes(1), new PhpBackend()))->read($rows);

        static::assertFalse($fits);
        static::assertCount(1, $read);
        static::assertSame([1, 2, 3, 4], IdRows::ids(BoundedRead::followedBy($read, $rows)));
    }

    public function test_followed_by_a_finished_stream_yields_only_what_was_read(): void
    {
        $rows = IdRows::batches([1]);
        [$read] = (new BoundedRead(Unit::fromGb(64), new PhpBackend()))->read($rows);

        static::assertSame([1], IdRows::ids(BoundedRead::followedBy($read, $rows)));
    }
}
