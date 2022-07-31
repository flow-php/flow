<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\Condition;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Condition\EntryValueEqualsTo;
use PHPUnit\Framework\TestCase;

final class EntryValueEqualsToTest extends TestCase
{
    public function test_same_values() : void
    {
        $this->assertTrue((new EntryValueEqualsTo('test', 1, true))->isMetFor(Row::create(Entry::integer('test', 1))));
        $this->assertFalse((new EntryValueEqualsTo('test', 1, true))->isMetFor(Row::create(Entry::integer('test', 2))));
    }

    public function test_similar_values() : void
    {
        $this->assertTrue((new EntryValueEqualsTo('test', 1, false))->isMetFor(Row::create(Entry::string('test', '1'))));
        $this->assertFalse((new EntryValueEqualsTo('test', 1, false))->isMetFor(Row::create(Entry::string('test', '2'))));
    }

    public function test_wrong_entry() : void
    {
        $this->assertFalse((new EntryValueEqualsTo('test', 1, true))->isMetFor(Row::create(Entry::integer('not-test', 1))));
    }
}
