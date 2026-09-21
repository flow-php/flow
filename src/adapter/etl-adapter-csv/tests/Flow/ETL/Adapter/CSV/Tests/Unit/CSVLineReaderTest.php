<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVLineReader;
use Flow\ETL\Adapter\CSV\Tests\Double\LengthCapturingSourceStream;
use Flow\Filesystem\Stream\MemorySourceStream;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function preg_last_error;
use function str_repeat;

use const PREG_NO_ERROR;

final class CSVLineReaderTest extends TestCase
{
    /**
     * @return Generator<string, array{string, string, string, non-empty-string, list<string>}>
     */
    public static function record_boundaries(): Generator
    {
        yield 'ends outside an enclosure' => [',', '"', '\\', "\"a\",b\nc,d", ['"a",b', 'c,d']];
        yield 'ends inside an enclosure' => [',', '"', '\\', "\"a\nb\",c\nd", ["\"a\nb\",c", 'd']];
        yield 'escaped enclosure' => [',', '"', '\\', "\"x\\\"y\nz\",1\n\"p\",2", ["\"x\\\"y\nz\",1", '"p",2']];
        yield 'escape before the line end' => [',', '"', '\\', "\"x\\\n\",1\n\"p\",2", ["\"x\\\n\",1", '"p",2']];
        yield 'doubled enclosure' => [',', '"', '\\', "\"a\"\"\nb\",1\n\"c\",2", ["\"a\"\"\nb\",1", '"c",2']];
        yield 'doubled enclosure at the buffer end' => [',', '"', '\\', "\"a\"\"\n\",1", ["\"a\"\"\n\",1"]];
        yield 'enclosure inside an unenclosed field' => [',', '"', '\\', "x\"y,1\n\"p\",2", ['x"y,1', '"p",2']];
        yield 'blanks before an opening enclosure' => [',', '"', '\\', "a, \t\"b\nc\"\nd", ["a, \t\"b\nc\"", 'd']];
        yield 'junk after a closing enclosure' => [',', '"', '\\', "\"a\"x\"y,1\nb", ['"a"x"y,1', 'b']];
        yield 'empty escape' => [',', '"', '', "\"x\\\",1\n\"p\",2", ['"x\\",1', '"p",2']];
        yield 'escape equal to the enclosure' => [',', '"', '"', "\"a\"\"\nb\",1\nc", ["\"a\"\"\nb\",1", 'c']];
        yield 'custom separator and enclosure' => [
            ';',
            "'",
            '\\',
            "x,'y;1\na;'b\nc';d\ne;f",
            ["x,'y;1", "a;'b\nc';d", 'e;f'],
        ];
    }

    public function test_a_record_ending_outside_an_enclosure_is_complete(): void
    {
        static::assertSame(
            ['"a",b', 'c,d'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("\"a\",b\nc,d"))),
        );
    }

    public function test_a_record_ending_inside_an_enclosure_is_incomplete(): void
    {
        static::assertSame(
            ["\"a\nb\",c", 'd'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("\"a\nb\",c\nd"))),
        );
    }

    public function test_an_escaped_enclosure_does_not_close_the_record(): void
    {
        static::assertSame(
            ["\"x\\\"y\nz\",1", '"p",2'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("\"x\\\"y\nz\",1\n\"p\",2"))),
        );
    }

    public function test_a_doubled_enclosure_does_not_close_the_record(): void
    {
        static::assertSame(
            ["\"a\"\"\nb\",1", '"c",2'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("\"a\"\"\nb\",1\n\"c\",2"))),
        );
    }

    public function test_an_enclosure_inside_an_unenclosed_field_is_a_literal_byte(): void
    {
        static::assertSame(
            ['x"y,1', '"p",2'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("x\"y,1\n\"p\",2"))),
        );
    }

    public function test_an_empty_escape_makes_the_escape_character_ordinary(): void
    {
        $content = "\"x\\\",1\n\"p\",2";

        static::assertSame(
            ["\"x\\\",1\n\"p\",2"],
            iterator_to_array((new CSVLineReader('"', escape: '\\'))->readLines(new MemorySourceStream($content))),
        );
        static::assertSame(
            ['"x\\",1', '"p",2'],
            iterator_to_array((new CSVLineReader('"', escape: ''))->readLines(new MemorySourceStream($content))),
        );
    }

    public function test_a_buffer_without_any_enclosure_is_complete(): void
    {
        static::assertSame(
            ['a,b', 'c,d'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream("a,b\nc,d"))),
        );
    }

    public function test_a_custom_separator_and_enclosure_are_honoured(): void
    {
        static::assertSame(
            ["x,'y;1", "a;'b\nc';d", 'e;f'],
            iterator_to_array((new CSVLineReader("'", ';'))->readLines(
                new MemorySourceStream("x,'y;1\na;'b\nc';d\ne;f"),
            )),
        );
    }

    public function test_a_buffer_of_one_megabyte_without_a_closing_enclosure_does_not_blow_the_pcre_backtrack_limit(): void
    {
        $open = '"' . str_repeat('ab,\\"""c', 1 << 17);

        static::assertSame(
            [$open . "\nd"],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream($open . "\nd"))),
        );
        static::assertSame(PREG_NO_ERROR, preg_last_error());
    }

    public function test_a_record_with_more_fields_than_pcre_can_match_is_still_split_at_its_end(): void
    {
        $closed = str_repeat('"a",', 200_000) . '"b"';
        $open = str_repeat('"a",', 200_000) . '"b';

        static::assertSame(
            [$closed, 'c'],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream($closed . "\nc"))),
        );
        static::assertSame(
            [$open . "\nc\""],
            iterator_to_array((new CSVLineReader('"'))->readLines(new MemorySourceStream($open . "\nc\""))),
        );
    }

    /**
     * @param non-empty-string $content
     * @param list<string> $expected
     */
    #[DataProvider('record_boundaries')]
    public function test_the_pattern_splits_records_at_their_end(
        string $separator,
        string $enclosure,
        string $escape,
        string $content,
        array $expected,
    ): void {
        static::assertSame(
            $expected,
            iterator_to_array((new CSVLineReader($enclosure, $separator, $escape))->readLines(
                new MemorySourceStream($content),
            )),
        );
    }

    public function test_characters_read_in_line_is_passed_through_to_the_stream(): void
    {
        $stream = new LengthCapturingSourceStream("id,name\n1,foo", path('s3://bucket/users.csv'));

        iterator_to_array((new CSVLineReader('"', charactersReadInLine: 4096))->readLines($stream));

        static::assertSame([4096], $stream->capturedLengths);
    }

    public function test_null_characters_read_in_line_lets_the_stream_choose_its_default(): void
    {
        $stream = new LengthCapturingSourceStream("id,name\n1,foo", path('s3://bucket/users.csv'));

        iterator_to_array((new CSVLineReader('"'))->readLines($stream));

        static::assertSame([null], $stream->capturedLengths);
    }

    public function test_detection_of_multiline_quotes(): void
    {
        $simpleContent = "field1,field2\nvalue1,value2";
        $simpleStream = new MemorySourceStream($simpleContent);

        $reader = new CSVLineReader('"');
        $simpleLines = iterator_to_array($reader->readLines($simpleStream));

        $multilineContent = "\"field1\",\"multi\nline\"";
        $multilineStream = new MemorySourceStream($multilineContent);

        $multilineLines = iterator_to_array($reader->readLines($multilineStream));

        static::assertCount(2, $simpleLines);
        static::assertCount(1, $multilineLines);
        static::assertSame('field1,field2', $simpleLines[0]);
        static::assertSame('value1,value2', $simpleLines[1]);
        static::assertSame("\"field1\",\"multi\nline\"", $multilineLines[0]);
    }

    public function test_performance_with_large_simple_csv(): void
    {
        $lines = [];

        for ($i = 0; $i < 1000; $i++) {
            $lines[] = "value{$i}a,value{$i}b,value{$i}c";
        }
        $csvContent = implode("\n", $lines);
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $result = iterator_to_array($reader->readLines($stream));

        static::assertCount(1000, $result);
        static::assertSame('value0a,value0b,value0c', $result[0]);
        static::assertSame('value999a,value999b,value999c', $result[999]);
    }

    public function test_reading_complex_multiline_csv(): void
    {
        $csvContent =
            '"artist","song","text"'
            . "\n"
            . '"ABBA","Song Title","Look at her face'
            . "\n"
            . 'And it means something special'
            . "\n"
            . 'How lucky can one fellow be?"'
            . "\n"
            . '"Another Artist","Another Song","Single line text"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('"artist","song","text"', $lines[0]);
        static::assertSame(
            '"ABBA","Song Title","Look at her face'
            . "\n"
            . 'And it means something special'
            . "\n"
            . 'How lucky can one fellow be?"',
            $lines[1],
        );
        static::assertSame('"Another Artist","Another Song","Single line text"', $lines[2]);
    }

    public function test_reading_csv_with_backslash_escaped_quotes(): void
    {
        $csvContent =
            'name,description'
            . "\n"
            . '"John \"The Great\"","A person with \"quotes\""'
            . "\n"
            . '"Jane","Normal person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('name,description', $lines[0]);
        static::assertSame('"John \"The Great\"","A person with \"quotes\""', $lines[1]);
        static::assertSame('"Jane","Normal person"', $lines[2]);
    }

    public function test_reading_csv_with_crlf_line_endings(): void
    {
        $csvContent = "\"header1\",\"header2\"\r\n\"value1\",\"value2\"\r\n\"value3\",\"multiline\r\nvalue\"";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('"header1","header2"', $lines[0]);
        static::assertSame('"value1","value2"', $lines[1]);
        static::assertSame("\"value3\",\"multiline\r\nvalue\"", $lines[2]);
    }

    public function test_reading_csv_with_custom_enclosure(): void
    {
        $csvContent = "name,description\n'John','First line\nSecond line'\n'Jane','Single line'";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader("'");
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('name,description', $lines[0]);
        static::assertSame("'John','First line\nSecond line'", $lines[1]);
        static::assertSame("'Jane','Single line'", $lines[2]);
    }

    public function test_reading_csv_with_escaped_quotes(): void
    {
        $csvContent =
            'name,description'
            . "\n"
            . '"John ""The Great""","A person with ""quotes"""'
            . "\n"
            . '"Jane","Normal person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('name,description', $lines[0]);
        static::assertSame('"John ""The Great""","A person with ""quotes"""', $lines[1]);
        static::assertSame('"Jane","Normal person"', $lines[2]);
    }

    public function test_reading_csv_with_multiline_quoted_fields(): void
    {
        $csvContent =
            'name,description' . "\n" . '"John","First line' . "\n" . 'Second line"' . "\n" . '"Jane","Single line"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('name,description', $lines[0]);
        static::assertSame('"John","First line' . "\n" . 'Second line"', $lines[1]);
        static::assertSame('"Jane","Single line"', $lines[2]);
    }

    public function test_reading_csv_with_only_header(): void
    {
        $csvContent = 'header1,header2,header3';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(1, $lines);
        static::assertSame('header1,header2,header3', $lines[0]);
    }

    public function test_reading_csv_with_quoted_fields(): void
    {
        $csvContent =
            'name,age,description' . "\n" . '"John Doe",30,"A nice person"' . "\n" . '"Jane Smith",25,"Another person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('name,age,description', $lines[0]);
        static::assertSame('"John Doe",30,"A nice person"', $lines[1]);
        static::assertSame('"Jane Smith",25,"Another person"', $lines[2]);
    }

    public function test_reading_csv_with_single_space(): void
    {
        $stream = new MemorySourceStream(' ');

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(1, $lines);
        static::assertSame(' ', $lines[0]);
    }

    public function test_reading_csv_with_trailing_newline(): void
    {
        $csvContent = "header1,header2\nvalue1,value2\n";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(2, $lines);
        static::assertSame('header1,header2', $lines[0]);
        static::assertSame('value1,value2', $lines[1]);
    }

    public function test_reading_simple_csv_without_quotes(): void
    {
        $csvContent = "header1,header2,header3\nvalue1,value2,value3\nvalue4,value5,value6";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        static::assertCount(3, $lines);
        static::assertSame('header1,header2,header3', $lines[0]);
        static::assertSame('value1,value2,value3', $lines[1]);
        static::assertSame('value4,value5,value6', $lines[2]);
    }
}
