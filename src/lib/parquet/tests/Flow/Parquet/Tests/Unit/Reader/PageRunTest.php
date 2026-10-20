<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Reader\PageRun;
use PHPUnit\Framework\TestCase;

final class PageRunTest extends TestCase
{
    public function test_an_extended_run_keeps_its_start(): void
    {
        $run = (new PageRun(100, 120))->extendedTo(150);

        static::assertSame([100, 150, 50], [$run->start, $run->end, $run->length]);
    }

    public function test_a_run_of_no_bytes_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The offset index locates a page of no bytes at offset 100');

        new PageRun(100, 100);
    }
}
