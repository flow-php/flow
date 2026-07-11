<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\PhpRowFrameEncoder;
use Flow\Floe\RowEncoder;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use PHPUnit\Framework\TestCase;

use function chr;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function pack;
use function strlen;

final class PhpRowFrameEncoderTest extends TestCase
{
    public function test_homogeneous_batch_produces_one_segment_with_the_section_schema(): void
    {
        $first = row(int_entry('id', 1), str_entry('name', 'a'));
        $second = row(int_entry('id', 2), str_entry('name', 'b'));

        $plan = FloeWriter::growSectionPlan(null, $first);
        $firstBody = (new RowEncoder())->encode($plan, $first);
        $secondBody = (new RowEncoder())->encode($plan, $second);

        $segments = (new PhpRowFrameEncoder())->encode(rows($first, $second));

        static::assertCount(1, $segments);
        static::assertSame($plan->schemaBody, $segments[0]->schemaBody);
        static::assertSame(2, $segments[0]->rowCount);
        static::assertSame(
            chr(Format::FRAME_ROW)
            . pack('V', strlen($firstBody))
            . $firstBody
            . chr(Format::FRAME_ROW)
            . pack('V', strlen($secondBody))
            . $secondBody,
            $segments[0]->frames,
        );
    }

    public function test_new_column_starts_a_new_segment_with_the_grown_union(): void
    {
        $first = row(int_entry('id', 1));
        $second = row(int_entry('id', 2), str_entry('name', 'flow'));

        $firstPlan = FloeWriter::growSectionPlan(null, $first);
        $secondPlan = FloeWriter::growSectionPlan($firstPlan->schemaBody, $second);
        $firstBody = (new RowEncoder())->encode($firstPlan, $first);
        $secondBody = (new RowEncoder())->encode($secondPlan, $second);

        $segments = (new PhpRowFrameEncoder())->encode(rows($first, $second));

        static::assertCount(2, $segments);
        static::assertSame($firstPlan->schemaBody, $segments[0]->schemaBody);
        static::assertSame(1, $segments[0]->rowCount);
        static::assertSame(chr(Format::FRAME_ROW) . pack('V', strlen($firstBody)) . $firstBody, $segments[0]->frames);
        static::assertSame($secondPlan->schemaBody, $segments[1]->schemaBody);
        static::assertSame(1, $segments[1]->rowCount);
        static::assertSame(chr(Format::FRAME_ROW) . pack('V', strlen($secondBody)) . $secondBody, $segments[1]->frames);
    }

    public function test_narrower_row_rides_the_current_section(): void
    {
        $first = row(int_entry('id', 1), str_entry('name', 'flow'));
        $second = row(int_entry('id', 2));

        $plan = FloeWriter::growSectionPlan(null, $first);
        $firstBody = (new RowEncoder())->encode($plan, $first);
        $secondBody = (new RowEncoder())->encode($plan, $second);

        $segments = (new PhpRowFrameEncoder())->encode(rows($first, $second));

        static::assertCount(1, $segments);
        static::assertSame($plan->schemaBody, $segments[0]->schemaBody);
        static::assertSame(2, $segments[0]->rowCount);
        static::assertSame(
            chr(Format::FRAME_ROW)
            . pack('V', strlen($firstBody))
            . $firstBody
            . chr(Format::FRAME_ROW)
            . pack('V', strlen($secondBody))
            . $secondBody,
            $segments[0]->frames,
        );
    }

    public function test_continuation_across_calls_yields_a_null_schema_body_segment(): void
    {
        $first = row(int_entry('id', 1));
        $second = row(int_entry('id', 2));

        $plan = FloeWriter::growSectionPlan(null, $first);
        $secondBody = (new RowEncoder())->encode($plan, $second);

        $encoder = new PhpRowFrameEncoder();
        $encoder->encode(rows($first));

        $segments = $encoder->encode(rows($second));

        static::assertCount(1, $segments);
        static::assertNull($segments[0]->schemaBody);
        static::assertSame(1, $segments[0]->rowCount);
        static::assertSame(chr(Format::FRAME_ROW) . pack('V', strlen($secondBody)) . $secondBody, $segments[0]->frames);
    }

    public function test_codec_is_applied_to_each_row_body(): void
    {
        $row = row(int_entry('id', 1));

        $plan = FloeWriter::growSectionPlan(null, $row);
        $body = "\xFF" . (new RowEncoder())->encode($plan, $row);

        $segments = (new PhpRowFrameEncoder(new PrefixingCodecStub()))->encode(rows($row));

        static::assertCount(1, $segments);
        static::assertSame(chr(Format::FRAME_ROW) . pack('V', strlen($body)) . $body, $segments[0]->frames);
    }

    public function test_empty_rows_produce_no_segments(): void
    {
        static::assertSame([], (new PhpRowFrameEncoder())->encode(rows()));
    }
}
