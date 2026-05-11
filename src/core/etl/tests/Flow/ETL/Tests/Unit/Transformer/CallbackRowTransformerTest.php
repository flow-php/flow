<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\CallbackRowTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;

class CallbackRowTransformerTest extends FlowTestCase
{
    public function test_replacing_dashes_in_entry_name_with_str_replace_callback(): void
    {
        $callbackTransformer = new CallbackRowTransformer(static fn(Row $row): Row => $row->remove('old-int'));

        $rows = $callbackTransformer->transform(
            rows(\Flow\ETL\DSL\row(integer_entry('old-int', 1000), string_entry('string-entry ', 'String entry'))),
            flow_context(config()),
        );

        static::assertEquals(rows(\Flow\ETL\DSL\row(string_entry('string-entry ', 'String entry'))), $rows);
    }
}
