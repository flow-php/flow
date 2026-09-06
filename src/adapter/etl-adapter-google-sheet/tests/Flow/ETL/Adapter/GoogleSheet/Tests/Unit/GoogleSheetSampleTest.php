<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetSample;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetSampleTest extends FlowTestCase
{
    public function test_carries_names_and_rows(): void
    {
        $sample = new GoogleSheetSample(['a'], [new RawRowValues(['a' => 1])], wholeSheet: true);

        static::assertSame(['a'], $sample->names);
        static::assertSame([['a' => 1]], [$sample->rows[0]->values]);
        static::assertTrue($sample->wholeSheet);
    }
}
