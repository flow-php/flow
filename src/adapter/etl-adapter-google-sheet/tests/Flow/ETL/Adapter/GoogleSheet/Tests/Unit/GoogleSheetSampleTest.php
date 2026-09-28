<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetSample;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetSampleTest extends FlowTestCase
{
    public function test_carries_names_and_rows(): void
    {
        $sample = new GoogleSheetSample(['a'], [['a' => 1]], wholeSheet: true, gridRowCount: 1000);

        static::assertSame(['a'], $sample->names);
        static::assertSame([['a' => 1]], [$sample->rows[0]]);
        static::assertTrue($sample->wholeSheet);
        static::assertSame(1000, $sample->gridRowCount);
    }
}
