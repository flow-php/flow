<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class TrimEndTest extends FlowTestCase
{
    public function test_trim_end_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   hello world   '],
                ['text' => '   php etl framework   '],
                ['text' => '   data processing   '],
            ]))
            ->withEntry('processed_text', ref('text')->trimEnd()->upper());

        self::assertEquals(
            [
                ['text' => '   hello world   ', 'processed_text' => '   HELLO WORLD'],
                ['text' => '   php etl framework   ', 'processed_text' => '   PHP ETL FRAMEWORK'],
                ['text' => '   data processing   ', 'processed_text' => '   DATA PROCESSING'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_code_formatting() : void
    {
        $df = df()
            ->from(from_array([
                ['code' => 'function test() { return true; }   '],
                ['code' => 'const value = "hello";   '],
                ['code' => 'if (condition) { doSomething(); }   '],
            ]))
            ->withEntry('clean_code', ref('code')->trimEnd());

        self::assertEquals(
            [
                ['code' => 'function test() { return true; }   ', 'clean_code' => 'function test() { return true; }'],
                ['code' => 'const value = "hello";   ', 'clean_code' => 'const value = "hello";'],
                ['code' => 'if (condition) { doSomething(); }   ', 'clean_code' => 'if (condition) { doSomething(); }'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_csv_data_cleaning() : void
    {
        $df = df()
            ->from(from_array([
                ['row' => 'Name,Age,City   '],
                ['row' => 'Product,Price,Stock   '],
                ['row' => 'ID,Description,Category   '],
            ]))
            ->withEntry('clean_row', ref('row')->trimEnd());

        self::assertEquals(
            [
                ['row' => 'Name,Age,City   ', 'clean_row' => 'Name,Age,City'],
                ['row' => 'Product,Price,Stock   ', 'clean_row' => 'Product,Price,Stock'],
                ['row' => 'ID,Description,Category   ', 'clean_row' => 'ID,Description,Category'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_currency_data() : void
    {
        $df = df()
            ->from(from_array([
                ['amount' => '123.45$$$'],
                ['amount' => '99.99$$'],
                ['amount' => '15.00$'],
            ]))
            ->withEntry('numeric_amount', ref('amount')->trimEnd('$'));

        self::assertEquals(
            [
                ['amount' => '123.45$$$', 'numeric_amount' => '123.45'],
                ['amount' => '99.99$$', 'numeric_amount' => '99.99'],
                ['amount' => '15.00$', 'numeric_amount' => '15.00'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_custom_characters() : void
    {
        $df = df()
            ->from(from_array([
                ['file' => 'document.txt.txt'],
                ['file' => 'image.png.png'],
                ['file' => 'script.js.js'],
            ]))
            ->withEntry('clean_file', ref('file')->trimEnd('.txt')->trimEnd('.png')->trimEnd('.js'));

        self::assertEquals(
            [
                ['file' => 'document.txt.txt', 'clean_file' => 'document'],
                ['file' => 'image.png.png', 'clean_file' => 'image'],
                ['file' => 'script.js.js', 'clean_file' => 'script'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_data_cleaning_pipeline() : void
    {
        $df = df()
            ->from(from_array([
                ['name' => '   John Doe   ', 'suffix' => '   Jr.   '],
                ['name' => '   Jane Smith   ', 'suffix' => '   Sr.   '],
                ['name' => '   Bob Wilson   ', 'suffix' => '   III   '],
            ]))
            ->withEntry('clean_name', ref('name')->trimEnd())
            ->withEntry('clean_suffix', ref('suffix')->trimEnd());

        self::assertEquals(
            [
                ['name' => '   John Doe   ', 'suffix' => '   Jr.   ', 'clean_name' => '   John Doe', 'clean_suffix' => '   Jr.'],
                ['name' => '   Jane Smith   ', 'suffix' => '   Sr.   ', 'clean_name' => '   Jane Smith', 'clean_suffix' => '   Sr.'],
                ['name' => '   Bob Wilson   ', 'suffix' => '   III   ', 'clean_name' => '   Bob Wilson', 'clean_suffix' => '   III'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_html_tag_cleaning() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => '<p>Paragraph content</p>   '],
                ['content' => '<div>Division content</div>   '],
                ['content' => '<span>Span content</span>   '],
            ]))
            ->withEntry('clean_content', ref('content')->trimEnd());

        self::assertEquals(
            [
                ['content' => '<p>Paragraph content</p>   ', 'clean_content' => '<p>Paragraph content</p>'],
                ['content' => '<div>Division content</div>   ', 'clean_content' => '<div>Division content</div>'],
                ['content' => '<span>Span content</span>   ', 'clean_content' => '<span>Span content</span>'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   Hello world   '],
                ['text' => 'Tabbed content\t\t\t'],
                ['text' => 'Newline content\n\n\n'],
            ]))
            ->withEntry('trimmed_end', ref('text')->trimEnd());

        self::assertEquals(
            [
                ['text' => '   Hello world   ', 'trimmed_end' => '   Hello world'],
                ['text' => 'Tabbed content\t\t\t', 'trimmed_end' => 'Tabbed content'],
                ['text' => 'Newline content\n\n\n', 'trimmed_end' => 'Newline content'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_json_formatting() : void
    {
        $df = df()
            ->from(from_array([
                ['json' => '{"key": "value"}   '],
                ['json' => '{"name": "test"}   '],
                ['json' => '{"id": 123}   '],
            ]))
            ->withEntry('clean_json', ref('json')->trimEnd());

        self::assertEquals(
            [
                ['json' => '{"key": "value"}   ', 'clean_json' => '{"key": "value"}'],
                ['json' => '{"name": "test"}   ', 'clean_json' => '{"name": "test"}'],
                ['json' => '{"id": 123}   ', 'clean_json' => '{"id": 123}'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_log_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['message' => 'Error occurred[/ERROR][/ERROR]'],
                ['message' => 'Info message[/INFO][/INFO]'],
                ['message' => 'Warning detected[/WARN][/WARN]'],
            ]))
            ->withEntry('clean_message', ref('message')->trimEnd('[/ERROR]')->trimEnd('[/INFO]')->trimEnd('[/WARN]'));

        self::assertEquals(
            [
                ['message' => 'Error occurred[/ERROR][/ERROR]', 'clean_message' => 'Error occurred'],
                ['message' => 'Info message[/INFO][/INFO]', 'clean_message' => 'Info message'],
                ['message' => 'Warning detected[/WARN][/WARN]', 'clean_message' => 'Warning detected'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_performance_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['text' => "   Text with trailing spaces {$i}   "];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('trimmed_text', ref('text')->trimEnd());

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals('   Text with trailing spaces 0', $result[0]['trimmed_text']);
        self::assertEquals('   Text with trailing spaces 999', $result[999]['trimmed_text']);
    }

    public function test_trim_end_sql_query_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['query' => 'SELECT * FROM users;;;'],
                ['query' => 'UPDATE users SET active = 1;;'],
                ['query' => 'DELETE FROM logs WHERE old = 1;'],
            ]))
            ->withEntry('clean_query', ref('query')->trimEnd(';'));

        self::assertEquals(
            [
                ['query' => 'SELECT * FROM users;;;', 'clean_query' => 'SELECT * FROM users'],
                ['query' => 'UPDATE users SET active = 1;;', 'clean_query' => 'UPDATE users SET active = 1'],
                ['query' => 'DELETE FROM logs WHERE old = 1;', 'clean_query' => 'DELETE FROM logs WHERE old = 1'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '   नमस्ते दुनिया   ', 'language' => 'Hindi'],
                ['text' => '   Здравствуй мир   ', 'language' => 'Russian'],
                ['text' => '   你好世界   ', 'language' => 'Chinese'],
            ]))
            ->withEntry('trimmed_text', ref('text')->trimEnd());

        self::assertEquals(
            [
                ['text' => '   नमस्ते दुनिया   ', 'language' => 'Hindi', 'trimmed_text' => '   नमस्ते दुनिया'],
                ['text' => '   Здравствуй мир   ', 'language' => 'Russian', 'trimmed_text' => '   Здравствуй мир'],
                ['text' => '   你好世界   ', 'language' => 'Chinese', 'trimmed_text' => '   你好世界'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_url_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['path' => 'example.com/path///'],
                ['path' => 'test.com/api//'],
                ['path' => 'site.com/'],
            ]))
            ->withEntry('clean_path', ref('path')->trimEnd('/'));

        self::assertEquals(
            [
                ['path' => 'example.com/path///', 'clean_path' => 'example.com/path'],
                ['path' => 'test.com/api//', 'clean_path' => 'test.com/api'],
                ['path' => 'site.com/', 'clean_path' => 'site.com'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => '   Valid content   '],
                ['content' => null],
                ['content' => '   Another valid   '],
            ]))
            ->withEntry('trimmed_content', ref('content')->trimEnd());

        self::assertEquals(
            [
                ['content' => '   Valid content   ', 'trimmed_content' => '   Valid content'],
                ['content' => null, 'trimmed_content' => null],
                ['content' => '   Another valid   ', 'trimmed_content' => '   Another valid'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_trim_end_with_scalar_function_parameter() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'Hello worldxxx', 'suffix' => 'xxx'],
                ['text' => 'Test data###', 'suffix' => '###'],
                ['text' => 'Important note!!!', 'suffix' => '!!!'],
            ]))
            ->withEntry('cleaned_text', ref('text')->trimEnd(ref('suffix')));

        self::assertEquals(
            [
                ['text' => 'Hello worldxxx', 'suffix' => 'xxx', 'cleaned_text' => 'Hello world'],
                ['text' => 'Test data###', 'suffix' => '###', 'cleaned_text' => 'Test data'],
                ['text' => 'Important note!!!', 'suffix' => '!!!', 'cleaned_text' => 'Important note'],
            ],
            $df->fetch()->toArray()
        );
    }
}
