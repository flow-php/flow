<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVLineReader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;

use Flow\Filesystem\Stream\NativeLocalSourceStream;

final class CSVLineReaderTest extends FlowTestCase
{
    public function test_memory_usage_with_large_multiline_csv() : void
    {
        $path = __DIR__ . '/../Fixtures/large_multiline_csv.csv';
        $memoryBefore = memory_get_usage(true);

        $stream = NativeLocalSourceStream::open(Path::realpath($path));
        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        $memoryAfter = memory_get_usage(true);
        $memoryUsed = $memoryAfter - $memoryBefore;

        self::assertCount(11, $lines);

        self::assertLessThan(50 * 1024 * 1024, $memoryUsed, 'Memory usage should be reasonable');

        self::assertSame('"id","content"', $lines[0]);
        self::assertStringStartsWith('"0","Line 0 content', $lines[1]);

        $stream->close();
    }

    public function test_reading_csv_with_custom_character_limit() : void
    {
        $path = __DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv';
        $stream = NativeLocalSourceStream::open(Path::realpath($path));

        $reader = new CSVLineReader('"', 2000);
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(2, $lines);
        self::assertGreaterThan(1000, strlen($lines[1])); // Check data line, not header

        $stream->close();
    }

    public function test_reading_csv_with_different_enclosures() : void
    {
        $path = __DIR__ . '/../Fixtures/single_quotes_csv.csv';
        $stream = NativeLocalSourceStream::open(Path::realpath($path));

        $reader = new CSVLineReader("'"); // Use single quote as enclosure
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('name,description', $lines[0]);
        self::assertSame("'John','Line 1\nLine 2'", $lines[1]);
        self::assertSame("'Jane','Single line'", $lines[2]);

        $stream->close();
    }

    public function test_reading_csv_with_escaped_quotes_file() : void
    {
        $path = __DIR__ . '/../Fixtures/escaped_quotes_csv.csv';
        $stream = NativeLocalSourceStream::open(Path::realpath($path));

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(3, $lines);
        self::assertSame('"name","description"', $lines[0]);
        self::assertSame('"John ""The Great""","Description with ""quotes"""', $lines[1]);
        self::assertSame('"Jane","Normal description"', $lines[2]);

        $stream->close();
    }

    public function test_reading_large_csv_file_performance() : void
    {
        $path = __DIR__ . '/../Fixtures/large_performance_csv.csv';
        $startTime = microtime(true);

        $stream = NativeLocalSourceStream::open(Path::realpath($path));
        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        self::assertCount(10001, $lines);

        self::assertLessThan(1.0, $executionTime, 'Reading 10,000 rows should complete in less than 1 second');

        self::assertSame('id,name,value,description', $lines[0]);
        self::assertSame('9999,name_9999,value_9999,description_9999', $lines[10000]);

        $stream->close();
    }

    public function test_reading_multiline_csv_file() : void
    {
        $path = __DIR__ . '/../Fixtures/multiline_strings.csv';
        $stream = NativeLocalSourceStream::open(Path::realpath($path));

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(2, $lines);

        self::assertSame('"artist","song","link","text"', $lines[0]);

        $dataLine = $lines[1];
        self::assertStringStartsWith('"ABBA","Ahe\'s My Kind Of Girl"', $dataLine);
        self::assertStringContainsString("Look at her face, it's a wonderful face", $dataLine);
        self::assertStringContainsString("\n", $dataLine); // Should contain actual newlines
        self::assertStringEndsWith('"', $dataLine);

        $stream->close();
    }

    public function test_reading_real_csv_file_with_quotes() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';
        $stream = NativeLocalSourceStream::open(Path::realpath($path));

        $reader = new CSVLineReader('"');
        $lines = iterator_to_array($reader->readLines($stream));

        self::assertCount(999, $lines);

        $expectedHeader = 'Year,Industry_aggregation_NZSIOC,Industry_code_NZSIOC,Industry_name_NZSIOC,Units,Variable_code,Variable_name,Variable_category,Value,Industry_code_ANZSIC06';
        self::assertSame($expectedHeader, rtrim($lines[0]));

        self::assertStringContainsString('"728,239"', $lines[1]);
        self::assertStringContainsString('"Sales, government funding, grants and subsidies"', $lines[2]);

        $stream->close();
    }
}
