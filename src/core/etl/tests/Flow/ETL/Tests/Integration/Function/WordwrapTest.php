<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class WordwrapTest extends FlowTestCase
{
    public function test_wordwrap_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'hello world test'],
                ['text' => 'php etl framework'],
                ['text' => 'data processing'],
            ]))
            ->withEntry('formatted_text', ref('text')->upper()->wordwrap(8));

        self::assertEquals(
            [
                ['text' => 'hello world test', 'formatted_text' => "HELLO\nWORLD\nTEST"],
                ['text' => 'php etl framework', 'formatted_text' => "PHP ETL\nFRAMEWORK"],
                ['text' => 'data processing', 'formatted_text' => "DATA\nPROCESSING"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_for_report_formatting() : void
    {
        $df = df()
            ->from(from_array([
                ['report_line' => 'Sales Report Q1 2023: Total Revenue $125,000', 'type' => 'header'],
                ['report_line' => 'Product A contributed 45% of total sales with $56,250', 'type' => 'detail'],
                ['report_line' => 'Product B contributed 35% of total sales with $43,750', 'type' => 'detail'],
                ['report_line' => 'Remaining 20% came from other products totaling $25,000', 'type' => 'detail'],
            ]))
            ->withEntry('formatted_line', ref('report_line')->wordwrap(40));

        $result = $df->fetch()->toArray();

        self::assertEquals(4, count($result));
        self::assertEquals("Sales Report Q1 2023: Total Revenue\n$125,000", $result[0]['formatted_line']);
        self::assertEquals("Product A contributed 45% of total\nsales with $56,250", $result[1]['formatted_line']);
        self::assertEquals("Product B contributed 35% of total\nsales with $43,750", $result[2]['formatted_line']);
        self::assertEquals("Remaining 20% came from other products\ntotaling $25,000", $result[3]['formatted_line']);
    }

    public function test_wordwrap_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'The quick brown fox jumps over the lazy dog', 'width' => 15],
                ['text' => 'Hello World Test', 'width' => 10],
                ['text' => 'PHP is awesome', 'width' => 8],
            ]))
            ->withEntry('wrapped_text', ref('text')->wordwrap(ref('width')));

        self::assertEquals(
            [
                ['text' => 'The quick brown fox jumps over the lazy dog', 'width' => 15, 'wrapped_text' => "The quick brown\nfox jumps over\nthe lazy dog"],
                ['text' => 'Hello World Test', 'width' => 10, 'wrapped_text' => "Hello\nWorld Test"],
                ['text' => 'PHP is awesome', 'width' => 8, 'wrapped_text' => "PHP is\nawesome"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_in_filtering_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'Short'],
                ['text' => 'This is a longer text'],
                ['text' => 'Medium length text'],
            ]))
            ->withEntry('wrapped_text', ref('text')->wordwrap(10))
            ->filter(ref('wrapped_text')->contains("\n"));

        self::assertEquals(
            [
                ['text' => 'This is a longer text', 'wrapped_text' => "This is a\nlonger\ntext"],
                ['text' => 'Medium length text', 'wrapped_text' => "Medium\nlength\ntext"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_performance_with_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['text' => "This is test number {$i} with some additional content to make it longer"];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('wrapped_text', ref('text')->wordwrap(20));

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals("This is test number 0\nwith some additional\ncontent to make it\nlonger", $result[0]['wrapped_text']);
        self::assertEquals("This is test number\n999 with some\nadditional content to\nmake it longer", $result[999]['wrapped_text']);
    }

    public function test_wordwrap_with_constant_width() : void
    {
        $df = df()
            ->from(from_array([
                ['description' => 'This is a very long description that needs to be wrapped'],
                ['description' => 'Short text'],
                ['description' => 'Another long description for testing word wrapping functionality'],
            ]))
            ->withEntry('formatted_description', ref('description')->wordwrap(20));

        self::assertEquals(
            [
                ['description' => 'This is a very long description that needs to be wrapped', 'formatted_description' => "This is a very long\ndescription that\nneeds to be wrapped"],
                ['description' => 'Short text', 'formatted_description' => 'Short text'],
                ['description' => 'Another long description for testing word wrapping functionality', 'formatted_description' => "Another long\ndescription for\ntesting word\nwrapping\nfunctionality"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_with_custom_break_character() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => 'Hello World Test', 'break' => ' | '],
                ['text' => 'PHP ETL Framework', 'break' => ' >>> '],
                ['text' => 'Data Processing Pipeline', 'break' => ' || '],
            ]))
            ->withEntry('formatted_text', ref('text')->wordwrap(12, ref('break')));

        self::assertEquals(
            [
                ['text' => 'Hello World Test', 'break' => ' | ', 'formatted_text' => 'Hello World | Test'],
                ['text' => 'PHP ETL Framework', 'break' => ' >>> ', 'formatted_text' => 'PHP ETL >>> Framework'],
                ['text' => 'Data Processing Pipeline', 'break' => ' || ', 'formatted_text' => 'Data || Processing || Pipeline'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_with_cut_option() : void
    {
        $df = df()
            ->from(from_array([
                ['word' => 'Supercalifragilisticexpialidocious', 'cut' => false],
                ['word' => 'antidisestablishmentarianism', 'cut' => true],
                ['word' => 'pneumonoultramicroscopicsilicovolcanoconiosiss', 'cut' => false],
            ]))
            ->withEntry('wrapped_word', ref('word')->wordwrap(10, "\n", ref('cut')));

        self::assertEquals(
            [
                ['word' => 'Supercalifragilisticexpialidocious', 'cut' => false, 'wrapped_word' => 'Supercalifragilisticexpialidocious'],
                ['word' => 'antidisestablishmentarianism', 'cut' => true, 'wrapped_word' => "antidisest\nablishment\narianism"],
                ['word' => 'pneumonoultramicroscopicsilicovolcanoconiosiss', 'cut' => false, 'wrapped_word' => 'pneumonoultramicroscopicsilicovolcanoconiosiss'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_with_html_content() : void
    {
        $df = df()
            ->from(from_array([
                ['html' => '<p>This is a very long paragraph that needs to be wrapped for better readability</p>'],
                ['html' => '<div>Short content</div>'],
                ['html' => '<span>Another long piece of content that should be wrapped properly</span>'],
            ]))
            ->withEntry('formatted_html', ref('html')->wordwrap(25));

        self::assertEquals(
            [
                ['html' => '<p>This is a very long paragraph that needs to be wrapped for better readability</p>', 'formatted_html' => "<p>This is a very long\nparagraph that needs to\nbe wrapped for better\nreadability</p>"],
                ['html' => '<div>Short content</div>', 'formatted_html' => '<div>Short content</div>'],
                ['html' => '<span>Another long piece of content that should be wrapped properly</span>', 'formatted_html' => "<span>Another long piece\nof content that should be\nwrapped properly</span>"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => 'Hello World Test'],
                ['content' => null],
                ['content' => 'Another test string'],
            ]))
            ->withEntry('wrapped_content', ref('content')->wordwrap(10));

        self::assertEquals(
            [
                ['content' => 'Hello World Test', 'wrapped_content' => "Hello\nWorld Test"],
                ['content' => null, 'wrapped_content' => null],
                ['content' => 'Another test string', 'wrapped_content' => "Another\ntest\nstring"],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_wordwrap_with_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['greeting' => 'नमस्ते दोस्त कैसे हैं आप आज', 'language' => 'Hindi'],
                ['greeting' => 'Здравствуй мой друг как дела', 'language' => 'Russian'],
                ['greeting' => '你好朋友今天怎么样', 'language' => 'Chinese'],
            ]))
            ->withEntry('wrapped_greeting', ref('greeting')->wordwrap(15));

        self::assertEquals(
            [
                ['greeting' => 'नमस्ते दोस्त कैसे हैं आप आज', 'language' => 'Hindi', 'wrapped_greeting' => "नमस्ते दोस्त कैसे\nहैं आप आज"],
                ['greeting' => 'Здравствуй мой друг как дела', 'language' => 'Russian', 'wrapped_greeting' => "Здравствуй мой\nдруг как дела"],
                ['greeting' => '你好朋友今天怎么样', 'language' => 'Chinese', 'wrapped_greeting' => '你好朋友今天怎么样'],
            ],
            $df->fetch()->toArray()
        );
    }
}
