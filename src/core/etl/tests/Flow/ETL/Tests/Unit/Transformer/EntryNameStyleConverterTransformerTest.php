<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Transformer\EntryNameStyleConverterTransformer;
use Flow\ETL\{Config, FlowContext, Function\StyleConverter\StringStyles, Row, Rows, Tests\FlowTestCase};

final class EntryNameStyleConverterTransformerTest extends FlowTestCase
{
    public function test_conversion_of_entry_names_style() : void
    {
        $transformer = new EntryNameStyleConverterTransformer(StringStyles::SNAKE);

        $rows = $transformer->transform(new Rows(
            Row::create(
                string_entry('CamelCaseEntryName', 'test'),
                string_entry('otherCaseEntryName', 'test'),
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
