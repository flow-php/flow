<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Converter;

use Flow\ETL\Row\Converter\ToJsonEntry;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class JsonConverterTest extends TestCase
{
    public function test_converts_collection_entry_into_json_entry() : void
    {
        $entry = new CollectionEntry(
            'items',
            new Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
            new Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
            new Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three'))
        );
        $converter = new ToJsonEntry();

        $this->assertEquals(
            new JsonEntry(
                'items',
                [
                    ['item-id' => 1, 'name' => 'one'],
                    ['item-id' => 2, 'name' => 'two'],
                    ['item-id' => 3, 'name' => 'three'],
                ]
            ),
            $converter->convert($entry)
        );
    }

    public function test_prevent_from_converting_non_collection_entry() : void
    {
        $converter = new ToJsonEntry();

        $this->expectExceptionMessage('Only "Flow\ETL\Row\Entry\CollectionEntry" can be transformed');

        $converter->convert(new StringEntry('name', 'just a string'));
    }
}
