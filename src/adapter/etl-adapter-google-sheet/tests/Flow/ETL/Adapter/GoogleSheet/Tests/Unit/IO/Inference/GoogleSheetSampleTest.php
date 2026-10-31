<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\IO\Inference;

use Flow\ETL\Adapter\GoogleSheet\IO\Inference\GoogleSheetSample;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetSampleTest extends FlowTestCase
{
    public function test_carries_names_and_rows(): void
    {
        $sample = new GoogleSheetSample(
            ['a'],
            [['a' => 1]],
            [1 => ['a'], 2 => [1]],
            wholeSheet: true,
            gridRowCount: 1000,
        );

        static::assertSame(['a'], $sample->names);
        static::assertSame([['a' => 1]], [$sample->rows[0]]);
        static::assertSame([1 => ['a'], 2 => [1]], $sample->cells);
        static::assertTrue($sample->wholeSheet);
        static::assertSame(1000, $sample->gridRowCount);
    }
}
