<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, rows};
use function Flow\ETL\DSL\{flow_context, integer_entry, string_entry};
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\{Row, Tests\FlowTestCase};

class CallbackRowTransformerTest extends FlowTestCase
{
    public function test_replacing_dashes_in_entry_name_with_str_replace_callback() : void
    {
        $callbackTransformer = new CallbackRowTransformer(
            fn (Row $row) : Row => $row->remove('old-int')
        );

        $rows = $callbackTransformer->transform(
            rows(\Flow\ETL\DSL\row(integer_entry('old-int', 1000), string_entry('string-entry ', 'String entry'))),
            flow_context(config())
        );

        static::assertEquals(rows(\Flow\ETL\DSL\row(string_entry('string-entry ', 'String entry'))), $rows);
    }
}
