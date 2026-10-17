<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Parquet\Engine\Arrow\ReadPlanConverter;
use Flow\Parquet\Reader\ReadPlan;
use PHPUnit\Framework\TestCase;

final class ReadPlanConverterTest extends TestCase
{
    public function test_the_selection_crosses_as_file_absolute_count_and_skip_pairs(): void
    {
        static::assertSame(
            ['selectors' => [[2, true], [3, false]]],
            ReadPlanConverter::toExtension(ReadPlan::window(5, 2)),
        );
    }
}
