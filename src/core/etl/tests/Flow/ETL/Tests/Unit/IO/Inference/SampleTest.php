<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnTypesMother;

final class SampleTest extends FlowTestCase
{
    public function test_negative_bytes_are_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sampled bytes must not be negative');

        // @mago-expect analysis:invalid-argument
        new Sample(ColumnTypesMother::fromStrings()->folded(), -1, true);
    }

    public function test_merge_widens_the_folds_sums_the_bytes_and_is_complete_only_when_both_were(): void
    {
        $first = ColumnTypesMother::fromStrings(['id']);
        $first->observe(['id' => '1']);
        $second = ColumnTypesMother::fromStrings(['id']);
        $second->observe(['id' => 'a']);
        $second->observe(['id' => 'b']);

        $merged = (new Sample($first->folded(), 2, true))->merge(new Sample($second->folded(), 4, false));

        static::assertEquals($first->folded()->merge($second->folded(), true), $merged->columns);
        static::assertSame(3, $merged->columns->rows);
        static::assertSame(6, $merged->bytes);
        static::assertFalse($merged->complete);
        static::assertTrue((new Sample($first->folded(), 1, true))->merge(
            new Sample($second->folded(), 1, true),
        )->complete);
    }

    public function test_normalize_round_trips(): void
    {
        $columns = ColumnTypesMother::fromStrings(['id']);
        $columns->observe(['id' => '1']);
        $sample = new Sample($columns->folded(), 12, false);

        static::assertSame(12, $sample->normalize()['bytes']);
        static::assertFalse($sample->normalize()['complete']);
        static::assertEquals($sample, Sample::fromArray($sample->normalize()));
    }
}
