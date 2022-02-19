<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\StringEntryValueCaseConverterTransformer;
use PHPUnit\Framework\TestCase;

final class StringEntryValueCaseConverterTransformerTest extends TestCase
{
    public function test_convert_entry_value_to_upper_case() : void
    {
        $transformer = StringEntryValueCaseConverterTransformer::upper('OtherEntryName', 'OtherEntryNameTest');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('EntryName', 'test'),
                new Row\Entry\StringEntry('OtherEntryName', 'test_test'),
                new Row\Entry\StringEntry('OtherEntryNameTest', 'test'),
            )
        ));

        $this->assertSame(
            [
                [
                    'EntryName' => 'test',
                    'OtherEntryName' => 'TEST_TEST',
                    'OtherEntryNameTest' => 'TEST',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_convert_entry_value_to_lower_case() : void
    {
        $transformer = StringEntryValueCaseConverterTransformer::lower('OtherEntryName', 'OtherEntryNameTest');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('EntryName', 'TEST'),
                new Row\Entry\StringEntry('OtherEntryName', 'TEST_TEST'),
                new Row\Entry\StringEntry('OtherEntryNameTest', 'test'),
            )
        ));

        $this->assertSame(
            [
                [
                    'EntryName' => 'TEST',
                    'OtherEntryName' => 'test_test',
                    'OtherEntryNameTest' => 'test',
                ],
            ],
            $rows->toArray()
        );
    }
}
