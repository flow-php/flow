<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader\SkippedMatches;
use PHPUnit\Framework\TestCase;

final class SkippedMatchesTest extends TestCase
{
    public function test_it_carries_the_matches_skipped(): void
    {
        static::assertSame(3, (new SkippedMatches(3))->count);
    }

    public function test_a_negative_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Skipped matches cannot be negative, given: -1');

        new SkippedMatches(-1);
    }
}
