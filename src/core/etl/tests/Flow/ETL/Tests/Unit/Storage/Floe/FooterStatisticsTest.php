<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Floe;

use Flow\ETL\Storage\Floe\FooterStatistics;
use Flow\ETL\Values\Column\Frame\FloeException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class FooterStatisticsTest extends TestCase
{
    public function test_a_missing_key_is_rejected(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe footer statistics are malformed');

        FooterStatistics::fromArray(['rows' => 1]);
    }

    public function test_an_unknown_key_is_ignored(): void
    {
        static::assertEquals(
            new FooterStatistics(3, 120),
            FooterStatistics::fromArray(['rows' => 3, 'byteSize' => 120, 'min' => 1]),
        );
    }

    public function test_it_normalizes_to_rows_and_byte_size(): void
    {
        static::assertSame(['rows' => 3, 'byteSize' => 120], (new FooterStatistics(3, 120))->normalize());
    }

    public function test_it_round_trips_through_from_array(): void
    {
        $statistics = new FooterStatistics(3, 120);

        static::assertEquals($statistics, FooterStatistics::fromArray($statistics->normalize()));
    }

    #[TestWith([-1, 0, 'Floe statistics rows must not be negative, given: -1'])]
    #[TestWith([0, -1, 'Floe statistics byte size must not be negative, given: -1'])]
    public function test_negative_values_are_rejected(int $rows, int $byteSize, string $message): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage($message);

        new FooterStatistics($rows, $byteSize);
    }
}
