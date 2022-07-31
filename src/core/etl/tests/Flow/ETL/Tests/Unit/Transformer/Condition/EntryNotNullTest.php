<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\Condition;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Condition\EntryNotNull;
use PHPUnit\Framework\TestCase;

final class EntryNotNullTest extends TestCase
{
    public function test_same_values() : void
    {
        $this->assertFalse((new EntryNotNull('test'))->isMetFor(Row::create(Entry::null('test'))));
        $this->assertTrue((new EntryNotNull('test'))->isMetFor(Row::create(Entry::integer('test', 2))));
    }

    public function test_wrong_entry() : void
    {
        $this->assertFalse((new EntryNotNull('test', 1, true))->isMetFor(Row::create(Entry::integer('not-test', 1))));
    }
}
