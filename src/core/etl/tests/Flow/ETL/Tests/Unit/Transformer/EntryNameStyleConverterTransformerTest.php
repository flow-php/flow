<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Transformer\EntryNameStyleConverterTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class EntryNameStyleConverterTransformerTest extends TestCase
{
    public function test_conversion_of_entry_names_style() : void
    {
        $transformer = new EntryNameStyleConverterTransformer(StringStyles::SNAKE);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('CamelCaseEntryName', 'test'),
                new Row\Entry\StringEntry('otherCaseEntryName', 'test'),
            )
        ), new FlowContext(Config::default()));

        self::assertSame(
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
