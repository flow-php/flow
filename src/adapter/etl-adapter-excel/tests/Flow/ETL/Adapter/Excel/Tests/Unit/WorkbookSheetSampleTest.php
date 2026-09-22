<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\WorkbookSheetSample;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function iterator_to_array;

final class WorkbookSheetSampleTest extends FlowTestCase
{
    public function test_a_sample_stopped_early_is_not_whole(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield new RawRowValues(['id' => 1]);
                yield new RawRowValues(['id' => 2]);
            })(),
        );

        foreach ($sample as $_rowValues) {
            break;
        }

        static::assertSame(1, $sample->rows());
        static::assertFalse($sample->wholeSheet());
    }

    public function test_a_sample_read_to_the_end_is_whole(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield new RawRowValues(['id' => 1]);
                yield new RawRowValues(['id' => 2]);
            })(),
        );

        iterator_to_array($sample, false);

        static::assertSame(2, $sample->rows());
        static::assertTrue($sample->wholeSheet());
    }

    public function test_nothing_is_counted_before_iteration(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield new RawRowValues(['id' => 1]);
            })(),
        );

        static::assertSame(0, $sample->rows());
        static::assertFalse($sample->wholeSheet());
    }
}
