<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, row, rows};
use function Flow\ETL\DSL\{flow_context, string_entry};
use Flow\ETL\Transformer\EntryNameStyleConverterTransformer;
use Flow\ETL\{Function\StyleConverter\StringStyles, Tests\FlowTestCase};

final class EntryNameStyleConverterTransformerTest extends FlowTestCase
{
    public function test_conversion_of_entry_names_style() : void
    {
        $transformer = new EntryNameStyleConverterTransformer(StringStyles::SNAKE);

        $rows = $transformer->transform(rows(row(string_entry('CamelCaseEntryName', 'test'), string_entry('otherCaseEntryName', 'test'))), flow_context(config()));

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
