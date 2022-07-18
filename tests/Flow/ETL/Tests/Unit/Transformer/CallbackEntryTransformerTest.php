<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;

class CallbackEntryTransformerTest extends TestCase
{
    public function test_removing_whitespace_with_trim_callback() : void
    {
        $callbackTransformer = Transform::callback_entry(
            fn (Entry $entry) : Entry => new $entry(\trim($entry->name()), $entry->value())
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string entry ', 'String entry')
                )
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string entry', 'String entry')
            )
        ), $rows);
    }

    public function test_removing_whitespace_with_trim_callback_with_serialization() : void
    {
        $callbackTransformer = Transform::callback_entry(
            fn (Entry $entry) : Entry => new $entry(\trim($entry->name()), $entry->value())
        );

        $serialization = new NativePHPSerializer();

        $rows = $serialization->unserialize($serialization->serialize($callbackTransformer))->transform(
            new Rows(
                Row::create(
                    new Entry\StringEntry('string entry ', 'String entry')
                )
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Entry\StringEntry('string entry', 'String entry')
            )
        ), $rows);
    }

    public function test_replacing_dashes_in_entry_name_with_str_replace_callback() : void
    {
        $callbackTransformer = Transform::callback_entry(
            fn (Entry $entry) : Entry => new $entry(\str_replace('-', '_', $entry->name()), $entry->value())
        );

        $rows = $callbackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old-int', 1000),
                    new Entry\StringEntry('string-entry ', 'String entry')
                )
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('old_int', 1000),
                new Entry\StringEntry('string_entry ', 'String entry')
            )
        ), $rows);
    }
}
