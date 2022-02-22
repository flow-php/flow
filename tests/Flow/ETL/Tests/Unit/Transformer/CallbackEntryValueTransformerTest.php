<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\CallbackEntryValueTransformer;
use PHPUnit\Framework\TestCase;

final class CallbackEntryValueTransformerTest extends TestCase
{
    public function test_upper_string_callback() : void
    {
        $callbackTransformer = new CallbackEntryValueTransformer(
            ['string-entry'],
            fn (Entry $entry) : Entry => new $entry($entry->name(), \strtoupper($entry->value()))
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old-int', 1000),
                    new Entry\StringEntry('string-entry', 'String entry')
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('old-int', 1000),
                new Entry\StringEntry('string-entry', 'STRING ENTRY')
            )
        ), $rows);
    }

    public function test_unique_array() : void
    {
        $callbackTransformer = new CallbackEntryValueTransformer(
            ['array_list'],
            fn (Entry $entry) : Entry => new $entry($entry->name(), \array_values(\array_unique($entry->value())))
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_list', [1, 1, 1, 2, 3, 4]),
                )
            )
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\ArrayEntry('array_list', [1, 2, 3, 4]),
            )
        ), $rows);
    }
}
