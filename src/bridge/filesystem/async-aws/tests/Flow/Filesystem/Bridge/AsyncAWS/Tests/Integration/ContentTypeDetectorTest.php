<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use Flow\Filesystem\Bridge\AsyncAWS\ContentTypeDetector;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Filesystem\Path\{Option, Options};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ContentTypeDetectorTest extends TestCase
{
    public static function provide_content_type_enum_values() : \Generator
    {
        yield 'CSV enum' => [ContentType::CSV, 'text/csv'];
        yield 'JSON enum' => [ContentType::JSON, 'application/json'];
        yield 'XML enum' => [ContentType::XML, 'application/xml'];
        yield 'PARQUET enum' => [ContentType::PARQUET, 'application/vnd.apache.parquet'];
        yield 'TEXT enum' => [ContentType::TEXT, 'text/plain'];
        yield 'TSV enum' => [ContentType::TSV, 'text/tab-separated-values'];
        yield 'ZIP enum' => [ContentType::ZIP, 'application/zip'];
        yield 'ORC enum' => [ContentType::ORC, 'application/vnd.apache.orc'];
        yield 'AVRO enum' => [ContentType::AVRO, 'application/vnd.apache.avro'];
        yield 'BINARY enum' => [ContentType::BINARY, 'application/octet-stream'];
    }

    public static function provide_file_extensions() : \Generator
    {
        yield 'csv extension' => ['file.csv', 'text/csv'];
        yield 'json extension' => ['file.json', 'application/json'];
        yield 'xml extension' => ['file.xml', 'application/xml'];
        yield 'parquet extension' => ['file.parquet', 'application/vnd.apache.parquet'];
        yield 'txt extension' => ['file.txt', 'text/plain'];
        yield 'tsv extension' => ['file.tsv', 'text/tab-separated-values'];
        yield 'zip extension' => ['file.zip', 'application/zip'];
        yield 'avro extension' => ['file.avro', 'application/avro'];
        yield 'orc extension' => ['file.orc', 'application/vnd.apache.orc'];
        yield 'unknown extension' => ['file.unknown', 'application/octet-stream'];
        yield 'no extension' => ['file', 'application/octet-stream'];
    }

    public static function provide_string_content_types() : \Generator
    {
        yield 'custom text/html' => ['text/html', 'text/html'];
        yield 'custom application/pdf' => ['application/pdf', 'application/pdf'];
        yield 'custom image/png' => ['image/png', 'image/png'];
        yield 'custom video/mp4' => ['video/mp4', 'video/mp4'];
    }

    public function test_all_enum_values_are_supported() : void
    {
        $detector = new ContentTypeDetector();

        $enumCases = ContentType::cases();

        foreach ($enumCases as $contentType) {
            $options = new Options([Option::CONTENT_TYPE->value => $contentType]);
            $path = new Path('aws-s3://bucket/file.dat', $options);

            $result = $detector->from($path);

            self::assertNotEmpty($result);
        }
    }

    public function test_all_supported_extensions_have_unique_mime_types() : void
    {
        $detector = new ContentTypeDetector();

        $extensions = [
            'csv' => 'text/csv',
            'json' => 'application/json',
            'xml' => 'application/xml',
            'parquet' => 'application/vnd.apache.parquet',
            'txt' => 'text/plain',
            'tsv' => 'text/tab-separated-values',
            'zip' => 'application/zip',
            'avro' => 'application/avro',
            'orc' => 'application/vnd.apache.orc',
        ];

        foreach ($extensions as $ext => $expectedMimeType) {
            $path = new Path("aws-s3://bucket/file.{$ext}");
            $result = $detector->from($path);

            self::assertSame($expectedMimeType, $result, "Extension '{$ext}' should map to '{$expectedMimeType}'");
        }
    }

    public function test_content_type_option_with_mixed_case_key() : void
    {
        $detector = new ContentTypeDetector();
        $options = new Options(['Content-Type' => 'application/json']);
        $path = new Path('aws-s3://bucket/file.dat', $options);

        $result = $detector->from($path);

        self::assertSame('application/json', $result);
    }

    public function test_detects_binary_content_type_for_unknown_extension() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/file.xyz');

        $result = $detector->from($path);

        self::assertSame('application/octet-stream', $result);
    }

    public function test_detects_content_type_for_local_path() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('file:///tmp/data.json');

        $result = $detector->from($path);

        self::assertSame('application/json', $result);
    }

    public function test_detects_content_type_for_nested_path() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/dir1/dir2/dir3/file.parquet');

        $result = $detector->from($path);

        self::assertSame('application/vnd.apache.parquet', $result);
    }

    public function test_detects_content_type_for_path_with_multiple_dots() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/file.backup.csv');

        $result = $detector->from($path);

        self::assertSame('text/csv', $result);
    }

    public function test_detects_content_type_for_path_without_trailing_slash() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/file.xml');

        $result = $detector->from($path);

        self::assertSame('application/xml', $result);
    }

    #[DataProvider('provide_content_type_enum_values')]
    public function test_detects_content_type_from_enum_option(ContentType $contentType, string $expectedMimeType) : void
    {
        $detector = new ContentTypeDetector();
        $options = new Options([Option::CONTENT_TYPE->value => $contentType]);
        $path = new Path('aws-s3://bucket/file.dat', $options);

        $result = $detector->from($path);

        self::assertSame($expectedMimeType, $result);
    }

    #[DataProvider('provide_file_extensions')]
    public function test_detects_content_type_from_file_extension(string $fileName, string $expectedMimeType) : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/' . $fileName);

        $result = $detector->from($path);

        self::assertSame($expectedMimeType, $result);
    }

    #[DataProvider('provide_string_content_types')]
    public function test_detects_content_type_from_string_option(string $contentType, string $expectedMimeType) : void
    {
        $detector = new ContentTypeDetector();
        $options = new Options([Option::CONTENT_TYPE->value => $contentType]);
        $path = new Path('aws-s3://bucket/file.dat', $options);

        $result = $detector->from($path);

        self::assertSame($expectedMimeType, $result);
    }

    public function test_detects_content_type_with_empty_options() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/file.json', new Options([]));

        $result = $detector->from($path);

        self::assertSame('application/json', $result);
    }

    public function test_detects_content_type_with_uppercase_extension() : void
    {
        $detector = new ContentTypeDetector();
        $path = new Path('aws-s3://bucket/file.CSV');

        $result = $detector->from($path);

        self::assertSame('text/csv', $result);
    }

    public function test_enum_option_takes_precedence_over_extension() : void
    {
        $detector = new ContentTypeDetector();
        $options = new Options([Option::CONTENT_TYPE->value => ContentType::JSON]);
        $path = new Path('aws-s3://bucket/file.csv', $options);

        $result = $detector->from($path);

        self::assertSame('application/json', $result);
    }

    public function test_string_option_takes_precedence_over_extension() : void
    {
        $detector = new ContentTypeDetector();
        $options = new Options([Option::CONTENT_TYPE->value => 'application/pdf']);
        $path = new Path('aws-s3://bucket/file.csv', $options);

        $result = $detector->from($path);

        self::assertSame('application/pdf', $result);
    }
}
