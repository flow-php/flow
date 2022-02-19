<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryNameStyleConverterTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use PHPUnit\Framework\TestCase;

final class EntryNameStyleConverterTransformerTest extends TestCase
{
    public function test_using_invalid_style() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unrecognized style wrong style, please use one of following: camel, pascal, snake, ada, macro, kebab, train, cobol, lower, upper, title, sentence');

        new EntryNameStyleConverterTransformer('wrong style');
    }

    public function test_conversion_of_entry_names_style() : void
    {
        $transformer = new EntryNameStyleConverterTransformer(StringStyles::SNAKE);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('CamelCaseEntryName', 'test'),
                new Row\Entry\StringEntry('otherCaseEntryName', 'test'),
            )
        ));

        $this->assertSame(
            [
                [
                    'camel_case_entry_name' => 'test',
                    'other_case_entry_name' => 'test',
                ],
            ],
            $rows->toArray()
        );
    }
}
