<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonReadBytes;
use Flow\ETL\Tests\FlowTestCase;

final class JsonReadBytesTest extends FlowTestCase
{
    public function test_it_starts_at_zero(): void
    {
        static::assertSame(0, (new JsonReadBytes())->total());
    }

    public function test_it_sums_what_was_added(): void
    {
        $read = new JsonReadBytes();
        $read->add(9);
        $read->add(10);

        static::assertSame(19, $read->total());
    }
}
