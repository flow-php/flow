<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Filter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use PHPUnit\Framework\TestCase;

final class CallbackTest extends TestCase
{
    public function test_callback_false() : void
    {
        $callback = new Callback(fn (Row $row) : bool => $row->valueOf('id') >= 1);

        $this->assertFalse($callback->keep(Row::create(Entry::integer('id', 0))));
    }

    public function test_callback_positive() : void
    {
        $callback = new Callback(fn (Row $row) : bool => $row->valueOf('id') >= 1);

        $this->assertTrue($callback->keep(Row::create(Entry::integer('id', 5))));
    }
}
