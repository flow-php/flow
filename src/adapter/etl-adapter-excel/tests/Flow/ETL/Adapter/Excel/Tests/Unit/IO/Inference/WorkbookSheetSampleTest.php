<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Inference;

use Flow\ETL\Adapter\Excel\IO\Inference\WorkbookSheetSample;
use Flow\ETL\Tests\FlowTestCase;

use function iterator_to_array;

final class WorkbookSheetSampleTest extends FlowTestCase
{
    public function test_a_sample_stopped_early_is_not_whole(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield ['id' => 1];
                yield ['id' => 2];
            })(),
        );

        foreach ($sample as $_rowValues) {
            break;
        }

        static::assertFalse($sample->wholeSheet());
    }

    public function test_a_sample_read_to_the_end_is_whole(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield ['id' => 1];
                yield ['id' => 2];
            })(),
        );

        iterator_to_array($sample, false);

        static::assertTrue($sample->wholeSheet());
    }

    public function test_a_sample_never_iterated_is_not_whole(): void
    {
        $sample = new WorkbookSheetSample(
            (static function () {
                yield ['id' => 1];
            })(),
        );

        static::assertFalse($sample->wholeSheet());
    }
}
