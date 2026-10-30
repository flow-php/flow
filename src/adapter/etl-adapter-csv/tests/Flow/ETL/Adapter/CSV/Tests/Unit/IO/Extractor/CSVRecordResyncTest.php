<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVRecordResync;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Filesystem\Tests\Double\ReadRecordingSourceStream;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function count;
use function Flow\Filesystem\DSL\path;
use function str_repeat;
use function strlen;

final class CSVRecordResyncTest extends TestCase
{
    public function test_a_cut_between_records_is_accepted_immediately(): void
    {
        $csv = "a,b\n1,2\n3,4\n5,6\n";

        static::assertSame(4, (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
            new StringSourceStream(path('memory://resync.csv'), $csv),
            4,
        ));
    }

    public function test_a_cut_inside_a_quoted_value_containing_newlines_is_not_fooled(): void
    {
        $inside = "id,text\n1,\"a";
        $record = $inside . "\nb,c\nd\"\n";

        static::assertSame(
            strlen($record),
            (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
                new StringSourceStream(path('memory://resync.csv'), $record . "2,x\n3,y\n4,z\n"),
                strlen($inside),
            ),
        );
    }

    public function test_a_cut_inside_a_quoted_value_containing_the_separator_is_not_fooled(): void
    {
        $inside = "id,text\n1,\"";
        $record = $inside . "p\nq,r\ns,t\"\n";

        static::assertSame(
            strlen($record),
            (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
                new StringSourceStream(path('memory://resync.csv'), $record . "2,x\n3,y\n4,z\n"),
                strlen($inside),
            ),
        );
    }

    public function test_a_cut_inside_a_quoted_value_skips_to_the_next_real_record(): void
    {
        $inside = "id,text\n1,\"hello ";
        $record = $inside . "world\"\n";

        static::assertSame(
            strlen($record),
            (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
                new StringSourceStream(path('memory://resync.csv'), $record . "2,x\n3,y\n4,z\n"),
                strlen($inside),
            ),
        );
    }

    /**
     * @param int<0, max> $from
     */
    #[TestWith([15])]
    #[TestWith([16])]
    #[TestWith([100])]
    public function test_a_cut_past_the_last_record_returns_null(int $from): void
    {
        static::assertNull((new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
            new StringSourceStream(path('memory://resync.csv'), "a,b\n1,2\n3,4\n5,6\n"),
            $from,
        ));
    }

    public function test_a_ragged_row_does_not_confirm_a_candidate(): void
    {
        $ragged = "a,b\n1,2\n3\n";

        static::assertSame(
            strlen($ragged),
            (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
                new StringSourceStream(path('memory://resync.csv'), $ragged . "4,5\n6,7\n8,9\n"),
                4,
            ),
        );
    }

    public function test_a_record_longer_than_a_read_chunk_is_found(): void
    {
        $long = str_repeat('x', 70_000);
        $csv = "a,b\n1," . $long . "\n2,y\n3,z\n";

        static::assertSame(
            strlen("a,b\n1," . $long . "\n"),
            (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
                new StringSourceStream(path('memory://resync.csv'), $csv),
                10,
            ),
        );
    }

    public function test_fewer_records_than_confirmations_before_the_end_are_enough(): void
    {
        static::assertSame(8, (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after(
            new StringSourceStream(path('memory://resync.csv'), "a,b\n1,2\n3,4"),
            5,
        ));
    }

    public function test_offset_zero_starts_a_record(): void
    {
        static::assertSame(0, (new CSVRecordResync(new CSVDialect(',', '"', '\\'), 5))->after(
            new StringSourceStream(path('memory://resync.csv'), "a,b\n"),
            0,
        ));
    }

    public function test_offset_zero_of_an_empty_stream_starts_nothing(): void
    {
        static::assertNull((new CSVRecordResync(new CSVDialect(',', '"', '\\'), 1))->after(
            new StringSourceStream(path('memory://resync.csv'), ''),
            0,
        ));
    }

    #[TestWith([0, 3, 'Expected fields must be greater than 0, given: 0'])]
    #[TestWith([2, 0, 'Confirmations must be greater than 0, given: 0'])]
    public function test_it_refuses_a_count_below_one(int $fields, int $confirmations, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        new CSVRecordResync(new CSVDialect(',', '"', '\\'), $fields, $confirmations);
    }

    public function test_rejected_candidates_cost_one_inner_read_per_block_not_per_line(): void
    {
        $stream = new ReadRecordingSourceStream(
            new StringSourceStream(path('memory://resync.csv'), "a,b\n" . str_repeat("1\n", 50_000)),
        );

        static::assertNull((new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2))->after($stream, 5));
        static::assertLessThan(5, count($stream->reads));
    }

    /**
     * @param int<1, max> $records
     */
    #[TestWith(["1,2\n3,4\n5,6\n", 2, 3, true], 'plain records')]
    #[TestWith(["1,2\n3\n5,6\n", 2, 3, false], 'a ragged record')]
    #[TestWith(["1,2\n3,4,5\n", 2, 3, false], 'a record too wide')]
    #[TestWith(["1,2\n\n\n3,4\n5,6\n", 2, 3, true], 'blank lines are skipped')]
    #[TestWith(["1,2\n3,4", 2, 3, true], 'the stream ends before the confirmations')]
    #[TestWith(['', 2, 3, false], 'an empty stream')]
    #[TestWith(["\n\n", 2, 3, false], 'only blank lines')]
    #[TestWith(["\"a\nb\",1\n2,3\n", 2, 2, true], 'an enclosed newline')]
    #[TestWith(["\"a,b\",1\n", 2, 1, true], 'an enclosed separator')]
    #[TestWith(["\"a\"\"b\",1\n", 2, 1, true], 'a doubled enclosure')]
    #[TestWith(["\"a\\\"b\",1\n", 2, 1, true], 'an escaped enclosure')]
    #[TestWith(["\"a\"\r\n", 1, 1, true], 'CRLF after an enclosure')]
    #[TestWith(["  \"a\",1\n", 2, 1, true], 'blanks before an opening enclosure')]
    #[TestWith(["b\"c,1\n", 2, 1, false], 'an enclosure inside an unenclosed field')]
    #[TestWith(["\"a\"x,1\n", 2, 1, false], 'text after a closing enclosure')]
    #[TestWith(["\"a,1\n", 2, 1, false], 'an enclosure never closed')]
    #[TestWith(["1,2\n3,4\n", 1, 1, false], 'the first record decides')]
    public function test_confirms_only_well_formed_records_of_the_expected_width(
        string $csv,
        int $fields,
        int $records,
        bool $confirms,
    ): void {
        static::assertSame($confirms, (new CSVRecordResync(
            new CSVDialect(',', '"', '\\'),
            $fields,
            $records,
        ))->confirms(new StringSourceStream(path('memory://shape.csv'), $csv), 0));
    }

    public function test_a_blank_separator_is_a_separator_not_a_blank(): void
    {
        static::assertTrue((new CSVRecordResync(new CSVDialect("\t", '"', '\\'), 3, 1))->confirms(
            new StringSourceStream(path('memory://shape.tsv'), "a\t\t\"b\"\n"),
            0,
        ));
    }

    public function test_an_escape_equal_to_the_enclosure_only_doubles(): void
    {
        static::assertTrue((new CSVRecordResync(new CSVDialect(',', '"', '"'), 2, 1))->confirms(
            new StringSourceStream(path('memory://shape.csv'), "\"a\"\"b\",1\n"),
            0,
        ));
    }

    public function test_it_reads_from_the_position(): void
    {
        static::assertTrue((new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2, 1))->confirms(
            new StringSourceStream(path('memory://shape.csv'), "garbage\"\n1,2\n"),
            9,
        ));
    }

    public function test_records_span_read_chunks(): void
    {
        static::assertTrue((new CSVRecordResync(new CSVDialect(',', '"', '\\'), 2, 2))->confirms(
            new StringSourceStream(path('memory://shape.csv'), '"' . str_repeat("x\n", 40_000) . "\",1\n2,3\n"),
            0,
        ));
    }
}
