<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\Condition;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Condition\EntryValueGreaterOrEqualThan;
use PHPUnit\Framework\TestCase;

final class EntryValueGreaterOrEqualThanTest extends TestCase
{
    public function test_greater_values() : void
    {
        $this->assertFalse((new EntryValueGreaterOrEqualThan('test', 5))->isMetFor(Row::create(Entry::integer('test', 1))));
        $this->assertTrue((new EntryValueGreaterOrEqualThan('test', 5))->isMetFor(Row::create(Entry::integer('test', 5))));
        $this->assertTrue((new EntryValueGreaterOrEqualThan('test', 5))->isMetFor(Row::create(Entry::integer('test', 10))));
    }

    public function test_wrong_entry() : void
    {
        $this->assertFalse((new EntryValueGreaterOrEqualThan('test', 1))->isMetFor(Row::create(Entry::integer('not-test', 1))));
    }
}
