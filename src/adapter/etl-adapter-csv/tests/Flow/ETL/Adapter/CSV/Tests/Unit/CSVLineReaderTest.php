<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVLineReader;
use Flow\Filesystem\Stream\MemorySourceStream;
use PHPUnit\Framework\TestCase;

final class CSVLineReaderTest extends TestCase
{
    public function test_detection_of_multiline_quotes() : void
    {
        $simpleContent = "field1,field2\nvalue1,value2";
        $simpleStream = new MemorySourceStream($simpleContent);

        $reader = new CSVLineReader('"');
        $simpleLines = iterator_to_array($reader->readLines($simpleStream));

        $multilineContent = "\"field1\",\"multi\nline\"";
        $multilineStream = new MemorySourceStream($multilineContent);

        $multilineLines = iterator_to_array($reader->readLines($multilineStream));

        self::assertCount(2, $simpleLines);
        self::assertCount(1, $multilineLines);
        self::assertSame('field1,field2', $simpleLines[0]);
        self::assertSame('value1,value2', $simpleLines[1]);
        self::assertSame("\"field1\",\"multi\nline\"", $multilineLines[0]);
    }

    public function test_performance_with_large_simple_csv() : void
    {
        $lines = [];

        for ($i = 0; $i < 1000; $i++) {
            $lines[] = "value{$i}a,value{$i}b,value{$i}c";
        }
        $csvContent = implode("\n", $lines);
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $result = iterator_to_array($reader->readLines($stream));

        self::assertCount(1000, $result);
        self::assertSame('value0a,value0b,value0c', $result[0]);
        self::assertSame('value999a,value999b,value999c', $result[999]);
    }

    public function test_reading_complex_multiline_csv() : void
    {
        $csvContent = '"artist","song","text"' . "\n" .
                     '"ABBA","Song Title","Look at her face' . "\n" .
                     'And it means something special' . "\n" .
                     'How lucky can one fellow be?"' . "\n" .
                     '"Another Artist","Another Song","Single line text"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('"artist","song","text"', $lines[0]);
        self::assertSame('"ABBA","Song Title","Look at her face' . "\n" . 'And it means something special' . "\n" . 'How lucky can one fellow be?"', $lines[1]);
        self::assertSame('"Another Artist","Another Song","Single line text"', $lines[2]);
    }

    public function test_reading_csv_with_backslash_escaped_quotes() : void
    {
        $csvContent = 'name,description' . "\n" .
                     '"John \"The Great\"","A person with \"quotes\""' . "\n" .
                     '"Jane","Normal person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,description', $lines[0]);
        self::assertSame('"John \"The Great\"","A person with \"quotes\""', $lines[1]);
        self::assertSame('"Jane","Normal person"', $lines[2]);
    }

    public function test_reading_csv_with_crlf_line_endings() : void
    {
        $csvContent = "\"header1\",\"header2\"\r\n\"value1\",\"value2\"\r\n\"value3\",\"multiline\r\nvalue\"";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('"header1","header2"', $lines[0]);
        self::assertSame('"value1","value2"', $lines[1]);
        self::assertSame("\"value3\",\"multiline\r\nvalue\"", $lines[2]);
    }

    public function test_reading_csv_with_custom_enclosure() : void
    {
        $csvContent = "name,description\n" .
                     "'John','First line\nSecond line'\n" .
                     "'Jane','Single line'";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader("'");
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,description', $lines[0]);
        self::assertSame("'John','First line\nSecond line'", $lines[1]);
        self::assertSame("'Jane','Single line'", $lines[2]);
    }

    public function test_reading_csv_with_escaped_quotes() : void
    {
        $csvContent = 'name,description' . "\n" .
                     '"John ""The Great""","A person with ""quotes"""' . "\n" .
                     '"Jane","Normal person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,description', $lines[0]);
        self::assertSame('"John ""The Great""","A person with ""quotes"""', $lines[1]);
        self::assertSame('"Jane","Normal person"', $lines[2]);
    }

    public function test_reading_csv_with_multiline_quoted_fields() : void
    {
        $csvContent = 'name,description' . "\n" .
                     '"John","First line' . "\n" . 'Second line"' . "\n" .
                     '"Jane","Single line"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,description', $lines[0]);
        self::assertSame('"John","First line' . "\n" . 'Second line"', $lines[1]);
        self::assertSame('"Jane","Single line"', $lines[2]);
    }

    public function test_reading_csv_with_only_header() : void
    {
        $csvContent = 'header1,header2,header3';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(1, $lines);
        self::assertSame('header1,header2,header3', $lines[0]);
    }

    public function test_reading_csv_with_quoted_fields() : void
    {
        $csvContent = 'name,age,description' . "\n" .
                     '"John Doe",30,"A nice person"' . "\n" .
                     '"Jane Smith",25,"Another person"';
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,age,description', $lines[0]);
        self::assertSame('"John Doe",30,"A nice person"', $lines[1]);
        self::assertSame('"Jane Smith",25,"Another person"', $lines[2]);
    }

    public function test_reading_csv_with_single_space() : void
    {
        $stream = new MemorySourceStream(' ');

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(1, $lines);
        self::assertSame(' ', $lines[0]);
    }

    public function test_reading_csv_with_trailing_newline() : void
    {
        $csvContent = "header1,header2\nvalue1,value2\n";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(2, $lines);
        self::assertSame('header1,header2', $lines[0]);
        self::assertSame('value1,value2', $lines[1]);
    }

    public function test_reading_simple_csv_without_quotes() : void
    {
        $csvContent = "header1,header2,header3\nvalue1,value2,value3\nvalue4,value5,value6";
        $stream = new MemorySourceStream($csvContent);

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('header1,header2,header3', $lines[0]);
        self::assertSame('value1,value2,value3', $lines[1]);
        self::assertSame('value4,value5,value6', $lines[2]);
    }
}
