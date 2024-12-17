<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Row\Entry;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

class CallbackRowTransformerTest extends TestCase
{
    public function test_replacing_dashes_in_entry_name_with_str_replace_callback() : void
    {
        $callbackTransformer = new CallbackRowTransformer(
            fn (Row $row) : Row => $row->remove('old-int')
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\IntegerEntry('old-int', 1000),
                    string_entry('string-entry ', 'String entry')
                )
            ),
            new FlowContext(Config::default())
        );

        static::assertEquals(new Rows(
            Row::create(
                string_entry('string-entry ', 'String entry')
            )
        ), $rows);
    }
}
