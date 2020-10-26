<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\Filter\TrimRowToContainOnly;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class TrimRowToContainOnlyTest extends TestCase
{
    public function test_trims_row_to_contain_only_given_entries() : void
    {
        $row = Row::create(
            $id = new IntegerEntry('id', 1234),
            new BooleanEntry('deleted', false),
            $createdAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
            new DateEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
            new NullEntry('phase'),
            $items = new CollectionEntry(
                'items',
                new Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
                new Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
                new Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three'))
            )
        );

        $this->assertEquals(
            Row::create($id, $createdAt, $items),
            (new TrimRowToContainOnly('id', 'created-at', 'items'))($row),
        );
    }
}
