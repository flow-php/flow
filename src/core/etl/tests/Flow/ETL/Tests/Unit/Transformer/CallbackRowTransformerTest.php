<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\CallbackRowTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;

class CallbackRowTransformerTest extends FlowTestCase
{
    public function test_replacing_dashes_in_entry_name_with_str_replace_callback(): void
    {
        $callbackTransformer = new CallbackRowTransformer(
            schema(string_schema('string-entry ')),
            static fn(Row $row): Row => row(['string-entry ' => $row->get('string-entry ')]),
        );

        $rows = $callbackTransformer->transform(
            rows(
                schema(integer_schema('old-int'), string_schema('string-entry ')),
                row(['old-int' => 1000, 'string-entry ' => 'String entry']),
            ),
            flow_context(config()),
        );

        static::assertEquals(
            rows(schema(string_schema('string-entry ')), row(['string-entry ' => 'String entry'])),
            $rows,
        );
    }
}
