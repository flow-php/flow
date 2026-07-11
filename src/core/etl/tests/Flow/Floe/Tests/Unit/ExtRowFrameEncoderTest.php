<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\ExtRowFrameEncoder;
use Flow\Floe\PhpRowFrameEncoder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class ExtRowFrameEncoderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('flow_php')) {
            static::markTestSkipped('flow_php extension is not loaded');
        }
    }

    public function test_homogeneous_batch_segments_are_identical_to_the_pure_php_engine(): void
    {
        $batch = rows(
            row(int_entry('id', 1), str_entry('name', 'a'), str_entry('nullable', null)),
            row(int_entry('id', 2), str_entry('name', 'b'), str_entry('nullable', 'x')),
        );

        static::assertEquals((new PhpRowFrameEncoder())->encode($batch), (new ExtRowFrameEncoder())->encode($batch));
    }

    public function test_new_column_segments_are_identical_to_the_pure_php_engine(): void
    {
        $batch = rows(row(int_entry('id', 1)), row(int_entry('id', 2), str_entry('name', 'flow')));

        static::assertEquals((new PhpRowFrameEncoder())->encode($batch), (new ExtRowFrameEncoder())->encode($batch));
    }

    public function test_narrower_row_segments_are_identical_to_the_pure_php_engine(): void
    {
        $batch = rows(row(int_entry('id', 1), str_entry('name', 'flow')), row(int_entry('id', 2)));

        static::assertEquals((new PhpRowFrameEncoder())->encode($batch), (new ExtRowFrameEncoder())->encode($batch));
    }

    public function test_continuation_across_calls_is_identical_to_the_pure_php_engine(): void
    {
        $first = rows(row(int_entry('id', 1)));
        $second = rows(row(int_entry('id', 2)));

        $php = new PhpRowFrameEncoder();
        $ext = new ExtRowFrameEncoder();

        static::assertEquals($php->encode($first), $ext->encode($first));
        static::assertEquals($php->encode($second), $ext->encode($second));
    }

    public function test_empty_rows_produce_no_segments(): void
    {
        static::assertSame([], (new ExtRowFrameEncoder())->encode(rows()));
    }
}
