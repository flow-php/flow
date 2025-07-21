<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class TrimStartTest extends FlowTestCase
{
    public function test_trim_start_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   hello world   '],
                ['text' => '   php etl framework   '],
                ['text' => '   data processing   '],
            ]))
            ->withEntry('processed_text', ref('text')->trimStart()->upper());

        self::assertEquals(
            [
                ['text' => '   hello world   ', 'processed_text' => 'HELLO WORLD   '],
                ['text' => '   php etl framework   ', 'processed_text' => 'PHP ETL FRAMEWORK   '],
                ['text' => '   data processing   ', 'processed_text' => 'DATA PROCESSING   '],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_csv_header_cleaning() : void
    {
        $df = df()
            ->from(from_array([
                ['header' => '   Name,Age,City   '],
                ['header' => '   Product,Price,Stock   '],
                ['header' => '   ID,Description,Category   '],
            ]))
            ->withEntry('clean_header', ref('header')->trimStart());

        self::assertEquals(
            [
                ['header' => '   Name,Age,City   ', 'clean_header' => 'Name,Age,City   '],
                ['header' => '   Product,Price,Stock   ', 'clean_header' => 'Product,Price,Stock   '],
                ['header' => '   ID,Description,Category   ', 'clean_header' => 'ID,Description,Category   '],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_currency_data() : void
    {
        $df = df()
            ->from(from_array([
                ['price' => '$$$123.45'],
                ['price' => '$$99.99'],
                ['price' => '$15.00'],
            ]))
            ->withEntry('numeric_price', ref('price')->trimStart('$'));

        self::assertEquals(
            [
                ['price' => '$$$123.45', 'numeric_price' => '123.45'],
                ['price' => '$$99.99', 'numeric_price' => '99.99'],
                ['price' => '$15.00', 'numeric_price' => '15.00'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_custom_characters() : void
    {
        $df = df()
            ->from(from_array([
                ['url' => 'http://example.com'],
                ['url' => 'https://test.com'],
                ['url' => 'ftp://files.com'],
            ]))
            ->withEntry('domain', ref('url')->trimStart('http://'));

        self::assertEquals(
            [
                ['url' => 'http://example.com', 'domain' => 'example.com'],
                ['url' => 'https://test.com', 'domain' => 'https://test.com'],
                ['url' => 'ftp://files.com', 'domain' => 'ftp://files.com'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_data_cleaning_pipeline() : void
    {
        $df = df()
            ->from(from_array([
                ['name' => '   John Doe   ', 'prefix' => '   Mr.   '],
                ['name' => '   Jane Smith   ', 'prefix' => '   Mrs.   '],
                ['name' => '   Bob Wilson   ', 'prefix' => '   Dr.   '],
            ]))
            ->withEntry('clean_name', ref('name')->trimStart())
            ->withEntry('clean_prefix', ref('prefix')->trimStart());

        self::assertEquals(
            [
                ['name' => '   John Doe   ', 'prefix' => '   Mr.   ', 'clean_name' => 'John Doe   ', 'clean_prefix' => 'Mr.   '],
                ['name' => '   Jane Smith   ', 'prefix' => '   Mrs.   ', 'clean_name' => 'Jane Smith   ', 'clean_prefix' => 'Mrs.   '],
                ['name' => '   Bob Wilson   ', 'prefix' => '   Dr.   ', 'clean_name' => 'Bob Wilson   ', 'clean_prefix' => 'Dr.   '],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_email_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['contact' => 'mailto:user@example.com'],
                ['contact' => 'mailto:admin@test.com'],
                ['contact' => 'mailto:support@company.com'],
            ]))
            ->withEntry('email', ref('contact')->trimStart('mailto:'));

        self::assertEquals(
            [
                ['contact' => 'mailto:user@example.com', 'email' => 'user@example.com'],
                ['contact' => 'mailto:admin@test.com', 'email' => 'admin@test.com'],
                ['contact' => 'mailto:support@company.com', 'email' => 'support@company.com'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_file_path_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['path' => '///path/to/file.txt'],
                ['path' => '//another/file.txt'],
                ['path' => '/single/slash/file.txt'],
            ]))
            ->withEntry('relative_path', ref('path')->trimStart('/'));

        self::assertEquals(
            [
                ['path' => '///path/to/file.txt', 'relative_path' => 'path/to/file.txt'],
                ['path' => '//another/file.txt', 'relative_path' => 'another/file.txt'],
                ['path' => '/single/slash/file.txt', 'relative_path' => 'single/slash/file.txt'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_html_tag_removal() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => '   <p>Paragraph content</p>'],
                ['content' => '   <div>Division content</div>'],
                ['content' => '   <span>Span content</span>'],
            ]))
            ->withEntry('clean_content', ref('content')->trimStart());

        self::assertEquals(
            [
                ['content' => '   <p>Paragraph content</p>', 'clean_content' => '<p>Paragraph content</p>'],
                ['content' => '   <div>Division content</div>', 'clean_content' => '<div>Division content</div>'],
                ['content' => '   <span>Span content</span>', 'clean_content' => '<span>Span content</span>'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   Hello world   '],
                ['text' => '\t\t\tTabbed content'],
                ['text' => '\n\n\nNewline content'],
            ]))
            ->withEntry('trimmed_start', ref('text')->trimStart());

        self::assertEquals(
            [
                ['text' => '   Hello world   ', 'trimmed_start' => 'Hello world   '],
                ['text' => '\t\t\tTabbed content', 'trimmed_start' => 'Tabbed content'],
                ['text' => '\n\n\nNewline content', 'trimmed_start' => 'Newline content'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_log_data_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['log' => '[ERROR]   Database connection failed'],
                ['log' => '[INFO]   User login successful'],
                ['log' => '[WARNING]   Memory usage high'],
            ]))
            ->withEntry('cleaned_log', ref('log')->trimStart('[ERROR]')->trimStart('[INFO]')->trimStart('[WARNING]'));

        self::assertEquals(
            [
                ['log' => '[ERROR]   Database connection failed', 'cleaned_log' => '   Database connection failed'],
                ['log' => '[INFO]   User login successful', 'cleaned_log' => '   User login successful'],
                ['log' => '[WARNING]   Memory usage high', 'cleaned_log' => '   Memory usage high'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_performance_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['text' => "   Text with leading spaces {$i}   "];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('trimmed_text', ref('text')->trimStart());

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals('Text with leading spaces 0   ', $result[0]['trimmed_text']);
        self::assertEquals('Text with leading spaces 999   ', $result[999]['trimmed_text']);
    }

    public function test_trim_start_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   नमस्ते दुनिया   ', 'language' => 'Hindi'],
                ['text' => '   Здравствуй мир   ', 'language' => 'Russian'],
                ['text' => '   你好世界   ', 'language' => 'Chinese'],
            ]))
            ->withEntry('trimmed_text', ref('text')->trimStart());

        self::assertEquals(
            [
                ['text' => '   नमस्ते दुनिया   ', 'language' => 'Hindi', 'trimmed_text' => 'नमस्ते दुनिया   '],
                ['text' => '   Здравствуй мир   ', 'language' => 'Russian', 'trimmed_text' => 'Здравствуй мир   '],
                ['text' => '   你好世界   ', 'language' => 'Chinese', 'trimmed_text' => '你好世界   '],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => '   Valid content   '],
                ['content' => null],
                ['content' => '   Another valid   '],
            ]))
            ->withEntry('trimmed_content', ref('content')->trimStart());

        self::assertEquals(
            [
                ['content' => '   Valid content   ', 'trimmed_content' => 'Valid content   '],
                ['content' => null, 'trimmed_content' => null],
                ['content' => '   Another valid   ', 'trimmed_content' => 'Another valid   '],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_start_with_scalar_function_parameter() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'xxxHello world', 'prefix' => 'xxx'],
                ['text' => '###Test data', 'prefix' => '###'],
                ['text' => '!!!Important note', 'prefix' => '!!!'],
            ]))
            ->withEntry('cleaned_text', ref('text')->trimStart(ref('prefix')));

        self::assertEquals(
            [
                ['text' => 'xxxHello world', 'prefix' => 'xxx', 'cleaned_text' => 'Hello world'],
                ['text' => '###Test data', 'prefix' => '###', 'cleaned_text' => 'Test data'],
                ['text' => '!!!Important note', 'prefix' => '!!!', 'cleaned_text' => 'Important note'],
            ],
            $df->fetch()->toArray()
        );
    }
}
