<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CollapseWhitespaceTest extends FlowTestCase
{
    public function test_address_normalization() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '123   Main   Street,   Apt   4B,   New   York,   NY'))
        );

        self::assertEquals('123 Main Street, Apt 4B, New York, NY', $result);
    }

    public function test_carriage_returns() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\r\r world"))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_code_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'function   test()   {    return    true;   }'))
        );

        self::assertEquals('function test() { return true; }', $result);
    }

    public function test_data_cleaning_scenario() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  John    Doe  ,   Software    Engineer  '))
        );

        self::assertEquals('John Doe , Software Engineer', $result);
    }

    public function test_description_text() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'This   is   a   very   long   description   with   extra   spaces.'))
        );

        self::assertEquals('This is a very long description with extra spaces.', $result);
    }

    public function test_email_addresses() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  user@example.com   test@domain.com  '))
        );

        self::assertEquals('user@example.com test@domain.com', $result);
    }

    public function test_emoji_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello   👋   World   🌍   Test   🚀'))
        );

        self::assertEquals('Hello 👋 World 🌍 Test 🚀', $result);
    }

    public function test_empty_string() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_form_field_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  John    Smith  '))
        );

        self::assertEquals('John Smith', $result);
    }

    public function test_html_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '<p>  Hello    world  </p>'))
        );

        self::assertEquals('<p> Hello world </p>', $result);
    }

    public function test_json_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '{  "name"  :   "John"  ,  "age"  :  30  }'))
        );

        self::assertEquals('{ "name" : "John" , "age" : 30 }', $result);
    }

    public function test_leading_and_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world   '))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_leading_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '   Hello world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_log_message_cleaning() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '[ERROR]   Database   connection   failed   at   2023-01-01'))
        );

        self::assertEquals('[ERROR] Database connection failed at 2023-01-01', $result);
    }

    public function test_mixed_content_types() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Email:   user@test.com   Phone:   555-1234   Age:   30'))
        );

        self::assertEquals('Email: user@test.com Phone: 555-1234 Age: 30', $result);
    }

    public function test_mixed_whitespace_types() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\t\tworld\n\ntest"))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_multiline_text() : void
    {
        $text = "Line 1\n\n\nLine 2\n\n  Line 3  ";
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', $text))
        );

        self::assertEquals('Line 1 Line 2 Line 3', $result);
    }

    public function test_multiple_spaces_between_words() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello     world     test'))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_newlines_and_spaces() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\n \n world"))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_numbers_and_text() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Price:   $123.45   Quantity:   10   '))
        );

        self::assertEquals('Price: $123.45 Quantity: 10', $result);
    }

    public function test_paragraph_text() : void
    {
        $text = "Lorem   ipsum   dolor   sit   amet,\n\n consectetur   adipiscing   elit.";
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', $text))
        );

        self::assertEquals('Lorem ipsum dolor sit amet, consectetur adipiscing elit.', $result);
    }

    public function test_phone_numbers() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '(555)   123-4567   ext   123'))
        );

        self::assertEquals('(555) 123-4567 ext 123', $result);
    }

    public function test_punctuation_handling() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello,   world!   How   are   you?'))
        );

        self::assertEquals('Hello, world! How are you?', $result);
    }

    public function test_report_data_cleaning() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Sales   Report:   Q1   2023   Revenue:   $125,000'))
        );

        self::assertEquals('Sales Report: Q1 2023 Revenue: $125,000', $result);
    }

    public function test_single_spaces() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world test'))
        );

        self::assertEquals('Hello world test', $result);
    }

    public function test_single_word() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello', $result);
    }

    public function test_single_word_with_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  Hello  '))
        );

        self::assertEquals('Hello', $result);
    }

    public function test_special_characters() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '@#$%^&*()   test   !@#$%^&*()'))
        );

        self::assertEquals('@#$%^&*() test !@#$%^&*()', $result);
    }

    public function test_sql_like_content() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'SELECT   *   FROM   users   WHERE   active   =   1'))
        );

        self::assertEquals('SELECT * FROM users WHERE active = 1', $result);
    }

    public function test_tabs_and_spaces() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello\t \t world"))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_title_normalization() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  Senior   Software   Engineer  '))
        );

        self::assertEquals('Senior Software Engineer', $result);
    }

    public function test_trailing_whitespace() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', 'Hello world   '))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  नमस्ते   दोस्त  '))
        );

        self::assertEquals('नमस्ते दोस्त', $result);
    }

    public function test_urls() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  https://example.com   https://test.com  '))
        );

        self::assertEquals('https://example.com https://test.com', $result);
    }

    public function test_user_input_normalization() : void
    {
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', '  first   name:   John   last   name:   Doe  '))
        );

        self::assertEquals('first name: John last name: Doe', $result);
    }

    public function test_very_long_spaces() : void
    {
        $spaces = str_repeat(' ', 100);
        $result = ref('str')->collapseWhitespace()->eval(
            row(str_entry('str', "Hello{$spaces}world"))
        );

        self::assertEquals('Hello world', $result);
    }
}
