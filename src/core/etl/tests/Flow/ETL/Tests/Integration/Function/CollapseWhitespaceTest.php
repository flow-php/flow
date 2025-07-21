<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, ref};
use Flow\ETL\Tests\FlowTestCase;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_collapse_whitespace_address_normalization() : void
    {
        $df = df()
            ->from(from_array([
                ['address' => '123   Main   Street,   Apt   4B,   New   York,   NY'],
                ['address' => '456   Oak   Avenue,   Suite   100,   Los   Angeles,   CA'],
                ['address' => '789   Pine   Road,   Unit   7,   Chicago,   IL'],
            ]))
            ->withEntry('normalized_address', ref('address')->collapseWhitespace());

        self::assertEquals(
            [
                ['address' => '123   Main   Street,   Apt   4B,   New   York,   NY', 'normalized_address' => '123 Main Street, Apt 4B, New York, NY'],
                ['address' => '456   Oak   Avenue,   Suite   100,   Los   Angeles,   CA', 'normalized_address' => '456 Oak Avenue, Suite 100, Los Angeles, CA'],
                ['address' => '789   Pine   Road,   Unit   7,   Chicago,   IL', 'normalized_address' => '789 Pine Road, Unit 7, Chicago, IL'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_chaining_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '  hello    world  '],
                ['text' => '  php   etl   framework  '],
                ['text' => '  data   processing  '],
            ]))
            ->withEntry('processed_text', ref('text')->collapseWhitespace()->upper());

        self::assertEquals(
            [
                ['text' => '  hello    world  ', 'processed_text' => 'HELLO WORLD'],
                ['text' => '  php   etl   framework  ', 'processed_text' => 'PHP ETL FRAMEWORK'],
                ['text' => '  data   processing  ', 'processed_text' => 'DATA PROCESSING'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_csv_data_cleaning() : void
    {
        $df = df()
            ->from(from_array([
                ['csv_row' => 'Name,   Age,   City'],
                ['csv_row' => 'John   Doe,   30,   New   York'],
                ['csv_row' => 'Jane   Smith,   25,   Los   Angeles'],
            ]))
            ->withEntry('cleaned_csv', ref('csv_row')->collapseWhitespace());

        self::assertEquals(
            [
                ['csv_row' => 'Name,   Age,   City', 'cleaned_csv' => 'Name, Age, City'],
                ['csv_row' => 'John   Doe,   30,   New   York', 'cleaned_csv' => 'John Doe, 30, New York'],
                ['csv_row' => 'Jane   Smith,   25,   Los   Angeles', 'cleaned_csv' => 'Jane Smith, 25, Los Angeles'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_data_cleaning_pipeline() : void
    {
        $df = df()
            ->from(from_array([
                ['name' => '  John    Doe  ', 'title' => '  Senior   Software   Engineer  '],
                ['name' => '  Jane   Smith  ', 'title' => '  Product   Manager  '],
                ['name' => '  Bob   Wilson  ', 'title' => '  Data   Analyst  '],
            ]))
            ->withEntry('clean_name', ref('name')->collapseWhitespace())
            ->withEntry('clean_title', ref('title')->collapseWhitespace());

        self::assertEquals(
            [
                ['name' => '  John    Doe  ', 'title' => '  Senior   Software   Engineer  ', 'clean_name' => 'John Doe', 'clean_title' => 'Senior Software Engineer'],
                ['name' => '  Jane   Smith  ', 'title' => '  Product   Manager  ', 'clean_name' => 'Jane Smith', 'clean_title' => 'Product Manager'],
                ['name' => '  Bob   Wilson  ', 'title' => '  Data   Analyst  ', 'clean_name' => 'Bob Wilson', 'clean_title' => 'Data Analyst'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_form_data_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['first_name' => '  John  ', 'last_name' => '  Doe  ', 'email' => '  john.doe@example.com  '],
                ['first_name' => '  Jane  ', 'last_name' => '  Smith  ', 'email' => '  jane.smith@test.com  '],
                ['first_name' => '  Bob  ', 'last_name' => '  Wilson  ', 'email' => '  bob.wilson@company.com  '],
            ]))
            ->withEntry('clean_first_name', ref('first_name')->collapseWhitespace())
            ->withEntry('clean_last_name', ref('last_name')->collapseWhitespace())
            ->withEntry('clean_email', ref('email')->collapseWhitespace());

        $result = $df->fetch()->toArray();

        self::assertEquals(3, count($result));
        self::assertEquals('John', $result[0]['clean_first_name']);
        self::assertEquals('Doe', $result[0]['clean_last_name']);
        self::assertEquals('john.doe@example.com', $result[0]['clean_email']);
    }

    public function test_collapse_whitespace_html_content_cleaning() : void
    {
        $df = df()
            ->from(from_array([
                ['html' => '<p>  Hello    world  </p>'],
                ['html' => '<div>  Multiple   spaces   here  </div>'],
                ['html' => '<span>  Text   normalization   example  </span>'],
            ]))
            ->withEntry('cleaned_html', ref('html')->collapseWhitespace());

        self::assertEquals(
            [
                ['html' => '<p>  Hello    world  </p>', 'cleaned_html' => '<p> Hello world </p>'],
                ['html' => '<div>  Multiple   spaces   here  </div>', 'cleaned_html' => '<div> Multiple spaces here </div>'],
                ['html' => '<span>  Text   normalization   example  </span>', 'cleaned_html' => '<span> Text normalization example </span>'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_in_dataframe_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '  Hello    world  '],
                ['text' => 'Multiple   spaces   between   words'],
                ['text' => "Mixed\t\twhitespace\n\ntypes"],
            ]))
            ->withEntry('cleaned_text', ref('text')->collapseWhitespace());

        self::assertEquals(
            [
                ['text' => '  Hello    world  ', 'cleaned_text' => 'Hello world'],
                ['text' => 'Multiple   spaces   between   words', 'cleaned_text' => 'Multiple spaces between words'],
                ['text' => "Mixed\t\twhitespace\n\ntypes", 'cleaned_text' => 'Mixed whitespace types'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_in_filtering_operations() : void
    {
        $df = df()
            ->from(from_array([
                ['message' => '  Important   message  '],
                ['message' => '   \t\n   '],  // whitespace only
                ['message' => '  Another   important   message  '],
            ]))
            ->withEntry('cleaned_message', ref('message')->collapseWhitespace())
            ->filter(ref('cleaned_message')->unicodeLength()->greaterThan(0));

        self::assertEquals(
            [
                ['message' => '  Important   message  ', 'cleaned_message' => 'Important message'],
                ['message' => '  Another   important   message  ', 'cleaned_message' => 'Another important message'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_log_processing() : void
    {
        $df = df()
            ->from(from_array([
                ['log_message' => '[ERROR]   Database   connection   failed   at   2023-01-01'],
                ['log_message' => '[INFO]   User   login   successful   for   user   123'],
                ['log_message' => '[WARNING]   High   memory   usage   detected'],
            ]))
            ->withEntry('cleaned_log', ref('log_message')->collapseWhitespace());

        self::assertEquals(
            [
                ['log_message' => '[ERROR]   Database   connection   failed   at   2023-01-01', 'cleaned_log' => '[ERROR] Database connection failed at 2023-01-01'],
                ['log_message' => '[INFO]   User   login   successful   for   user   123', 'cleaned_log' => '[INFO] User login successful for user 123'],
                ['log_message' => '[WARNING]   High   memory   usage   detected', 'cleaned_log' => '[WARNING] High memory usage detected'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_performance_with_large_dataset() : void
    {
        $data = [];

        for ($i = 0; $i < 1000; $i++) {
            $data[] = ['text' => "  Text   with   multiple   spaces   {$i}  "];
        }

        $df = df()
            ->from(from_array($data))
            ->withEntry('cleaned_text', ref('text')->collapseWhitespace());

        $result = $df->fetch()->toArray();

        self::assertCount(1000, $result);
        self::assertEquals('Text with multiple spaces 0', $result[0]['cleaned_text']);
        self::assertEquals('Text with multiple spaces 999', $result[999]['cleaned_text']);
    }

    public function test_collapse_whitespace_report_formatting() : void
    {
        $df = df()
            ->from(from_array([
                ['report_line' => 'Sales   Report:   Q1   2023   Revenue:   $125,000'],
                ['report_line' => 'Product   A   contributed   45%   of   total   sales'],
                ['report_line' => 'Customer   satisfaction   rating:   4.8/5.0'],
            ]))
            ->withEntry('formatted_line', ref('report_line')->collapseWhitespace());

        self::assertEquals(
            [
                ['report_line' => 'Sales   Report:   Q1   2023   Revenue:   $125,000', 'formatted_line' => 'Sales Report: Q1 2023 Revenue: $125,000'],
                ['report_line' => 'Product   A   contributed   45%   of   total   sales', 'formatted_line' => 'Product A contributed 45% of total sales'],
                ['report_line' => 'Customer   satisfaction   rating:   4.8/5.0', 'formatted_line' => 'Customer satisfaction rating: 4.8/5.0'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_unicode_content() : void
    {
        $df = df()
            ->from(from_array([
                ['text' => '  नमस्ते   दोस्त   कैसे   हैं  ', 'language' => 'Hindi'],
                ['text' => '  Здравствуй   мой   друг  ', 'language' => 'Russian'],
                ['text' => '  你好   朋友   今天   怎么样  ', 'language' => 'Chinese'],
            ]))
            ->withEntry('normalized_text', ref('text')->collapseWhitespace());

        self::assertEquals(
            [
                ['text' => '  नमस्ते   दोस्त   कैसे   हैं  ', 'language' => 'Hindi', 'normalized_text' => 'नमस्ते दोस्त कैसे हैं'],
                ['text' => '  Здравствуй   мой   друг  ', 'language' => 'Russian', 'normalized_text' => 'Здравствуй мой друг'],
                ['text' => '  你好   朋友   今天   怎么样  ', 'language' => 'Chinese', 'normalized_text' => '你好 朋友 今天 怎么样'],
            ],
            $df->fetch()->toArray()
        );
    }

    public function test_collapse_whitespace_with_null_values() : void
    {
        $df = df()
            ->from(from_array([
                ['content' => '  Hello    world  '],
                ['content' => null],
                ['content' => '  Another   test   string  '],
            ]))
            ->withEntry('normalized_content', ref('content')->collapseWhitespace());

        self::assertEquals(
            [
                ['content' => '  Hello    world  ', 'normalized_content' => 'Hello world'],
                ['content' => null, 'normalized_content' => null],
                ['content' => '  Another   test   string  ', 'normalized_content' => 'Another test string'],
            ],
            $df->fetch()->toArray()
        );
    }
}
