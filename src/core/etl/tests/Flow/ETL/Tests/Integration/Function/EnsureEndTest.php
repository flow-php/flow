<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class EnsureEndTest extends FlowTestCase
{
    public function test_ensure_end_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['css' => 'color: red'],
                ['css' => 'background: blue'],
                ['css' => 'margin: 10px;'],
            ]))
            ->withEntry('normalized_css', ref('css')->ensureEnd(';')->upper());

        self::assertEquals(
            [
                ['css' => 'color: red', 'normalized_css' => 'COLOR: RED;'],
                ['css' => 'background: blue', 'normalized_css' => 'BACKGROUND: BLUE;'],
                ['css' => 'margin: 10px;', 'normalized_css' => 'MARGIN: 10PX;'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['filename' => 'document', 'extension' => '.txt'],
                ['filename' => 'image.png', 'extension' => '.txt'],
                ['filename' => 'config.yml', 'extension' => '.txt'],
            ]))
            ->withEntry('normalized_filename', ref('filename')->ensureEnd(ref('extension')));

        self::assertEquals(
            [
                ['filename' => 'document', 'extension' => '.txt', 'normalized_filename' => 'document.txt'],
                ['filename' => 'image.png', 'extension' => '.txt', 'normalized_filename' => 'image.png.txt'],
                ['filename' => 'config.yml', 'extension' => '.txt', 'normalized_filename' => 'config.yml.txt'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_in_filtering_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['url' => 'https://example.com/api'],
                ['url' => 'https://secure.com/'],
                ['url' => 'https://test.com/path'],
            ]))
            ->withEntry('normalized_url', ref('url')->ensureEnd('/'))
            ->filter(ref('normalized_url')->endsWith('/'));

        self::assertEquals(
            [
                ['url' => 'https://example.com/api', 'normalized_url' => 'https://example.com/api/'],
                ['url' => 'https://secure.com/', 'normalized_url' => 'https://secure.com/'],
                ['url' => 'https://test.com/path', 'normalized_url' => 'https://test.com/path/'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_performance_with_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['filename' => "file{$i}"];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('log_filename', ref('filename')->ensureEnd('.log'));

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals('file0.log', $result[0]['log_filename']);
        self::assertEquals('file999.log', $result[999]['log_filename']);
    }

    public function test_ensure_end_with_aggregation_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['category' => 'tech', 'title' => 'PHP Features'],
                ['category' => 'business', 'title' => 'Market Analysis [BUSINESS]'],
                ['category' => 'tech', 'title' => 'New Framework'],
            ]))
            ->withEntry('categorized_title', ref('title')->ensureEnd(ref('category')->upper()->prepend(' [')->append(']')));

        self::assertEquals(
            [
                ['category' => 'tech', 'title' => 'PHP Features', 'categorized_title' => 'PHP Features [TECH]'],
                ['category' => 'business', 'title' => 'Market Analysis [BUSINESS]', 'categorized_title' => 'Market Analysis [BUSINESS]'],
                ['category' => 'tech', 'title' => 'New Framework', 'categorized_title' => 'New Framework [TECH]'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_constant_suffix() : void
    {
        $df = df()
            ->from(from_array([
                ['sql' => 'SELECT * FROM users'],
                ['sql' => 'UPDATE users SET active = 1;'],
                ['sql' => 'DELETE FROM logs'],
            ]))
            ->withEntry('normalized_sql', ref('sql')->ensureEnd(';'));

        self::assertEquals(
            [
                ['sql' => 'SELECT * FROM users', 'normalized_sql' => 'SELECT * FROM users;'],
                ['sql' => 'UPDATE users SET active = 1;', 'normalized_sql' => 'UPDATE users SET active = 1;'],
                ['sql' => 'DELETE FROM logs', 'normalized_sql' => 'DELETE FROM logs;'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_empty_and_null_suffixes() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'hello', 'suffix' => ''],
                ['text' => 'world', 'suffix' => null],
                ['text' => 'test', 'suffix' => '_suffix'],
            ]))
            ->withEntry('processed_text', ref('text')->ensureEnd(ref('suffix')));

        self::assertEquals(
            [
                ['text' => 'hello', 'suffix' => '', 'processed_text' => 'hello'],
                ['text' => 'world', 'suffix' => null, 'processed_text' => 'world'],
                ['text' => 'test', 'suffix' => '_suffix', 'processed_text' => 'test_suffix'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_file_extensions() : void
    {
        $df = df()
            ->from(from_array([
                ['filename' => 'document', 'type' => 'text'],
                ['filename' => 'image.png', 'type' => 'image'],
                ['filename' => 'script.js', 'type' => 'script'],
            ]))
            ->withEntry('normalized_filename', ref('filename')->ensureEnd('.log'));

        self::assertEquals(
            [
                ['filename' => 'document', 'type' => 'text', 'normalized_filename' => 'document.log'],
                ['filename' => 'image.png', 'type' => 'image', 'normalized_filename' => 'image.png.log'],
                ['filename' => 'script.js', 'type' => 'script', 'normalized_filename' => 'script.js.log'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_multiple_transformations() : void
    {
        $df = df()
            ->from(from_array([
                ['html_content' => '<p>Hello', 'css_rule' => 'color: blue'],
                ['html_content' => '<div>Content</div>', 'css_rule' => 'margin: 5px;'],
                ['html_content' => '<span>Text</span>', 'css_rule' => 'font-size: 14px'],
            ]))
            ->withEntry('normalized_html', ref('html_content')->ensureEnd('</p>'))
            ->withEntry('normalized_css', ref('css_rule')->ensureEnd(';'));

        self::assertEquals(
            [
                ['html_content' => '<p>Hello', 'css_rule' => 'color: blue', 'normalized_html' => '<p>Hello</p>', 'normalized_css' => 'color: blue;'],
                ['html_content' => '<div>Content</div>', 'css_rule' => 'margin: 5px;', 'normalized_html' => '<div>Content</div></p>', 'normalized_css' => 'margin: 5px;'],
                ['html_content' => '<span>Text</span>', 'css_rule' => 'font-size: 14px', 'normalized_html' => '<span>Text</span></p>', 'normalized_css' => 'font-size: 14px;'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'hello world'],
                ['text' => null],
                ['text' => 'test string'],
            ]))
            ->withEntry('suffixed_text', ref('text')->ensureEnd(' <<<'));

        self::assertEquals(
            [
                ['text' => 'hello world', 'suffixed_text' => 'hello world <<<'],
                ['text' => null, 'suffixed_text' => null],
                ['text' => 'test string', 'suffixed_text' => 'test string <<<'],
            ],
            $df->fetch()
        );
    }

    public function test_ensure_end_with_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['greeting' => 'नमस्ते', 'suffix' => ' जी'],
                ['greeting' => 'नमस्ते जी', 'suffix' => ' जी'],
                ['greeting' => 'hello', 'suffix' => ' 🎉'],
            ]))
            ->withEntry('full_greeting', ref('greeting')->ensureEnd(ref('suffix')));

        self::assertEquals(
            [
                ['greeting' => 'नमस्ते', 'suffix' => ' जी', 'full_greeting' => 'नमस्ते जी'],
                ['greeting' => 'नमस्ते जी', 'suffix' => ' जी', 'full_greeting' => 'नमस्ते जी'],
                ['greeting' => 'hello', 'suffix' => ' 🎉', 'full_greeting' => 'hello 🎉'],
            ],
            $df->fetch()
        );
    }
}
