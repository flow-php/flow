<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DataPageHeaderV2Test extends TestCase
{
    #[TestWith([null, true])]
    #[TestWith([true, true])]
    #[TestWith([false, false])]
    public function test_is_compressed_reads_a_missing_flag_as_true(?bool $flag, bool $expected): void
    {
        static::assertSame(
            $expected,
            (new DataPageHeaderV2(2, 0, 2, Encodings::PLAIN, 0, 0, $flag, null))->isCompressed(),
        );
    }
}
