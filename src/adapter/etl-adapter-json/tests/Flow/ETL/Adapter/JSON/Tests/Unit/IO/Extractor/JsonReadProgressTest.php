<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonReadProgress;
use Flow\ETL\Tests\FlowTestCase;

final class JsonReadProgressTest extends FlowTestCase
{
    public function test_it_starts_at_zero(): void
    {
        static::assertSame(0, (new JsonReadProgress())->total());
    }

    public function test_it_sums_what_was_added(): void
    {
        $read = new JsonReadProgress();
        $read->add(9);
        $read->add(10);

        static::assertSame(19, $read->total());
    }

    public function test_the_last_record_is_the_one_recorded_last(): void
    {
        $read = new JsonReadProgress();
        $read->recordAt(0, 1, '{"id":1}');
        $read->recordAt(9, 2, '{"id":2}');

        static::assertSame([9, 2, '{"id":2}'], [$read->lastOffset(), $read->lastLine(), $read->lastRecord()]);
    }

    public function test_nothing_recorded_is_offset_zero_without_a_line_or_a_record(): void
    {
        $read = new JsonReadProgress();

        static::assertSame([0, null, null], [$read->lastOffset(), $read->lastLine(), $read->lastRecord()]);
    }
}
