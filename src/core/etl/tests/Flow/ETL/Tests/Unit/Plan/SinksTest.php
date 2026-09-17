<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;
use function iterator_to_array;

final class SinksTest extends FlowTestCase
{
    public function test_an_empty_collection_holds_nothing(): void
    {
        $sinks = new Sinks();

        static::assertSame([], $sinks->all());
        static::assertCount(0, $sinks);
    }

    public function test_it_keeps_the_sinks_in_the_order_given(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $sinks = new Sinks($write, $transaction);

        static::assertSame([$write, $transaction], $sinks->all());
        static::assertSame([$write, $transaction], iterator_to_array($sinks));
        static::assertCount(2, $sinks);
    }

    public function test_merge_appends_the_given_sinks_after_its_own(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));
        $sinks = new Sinks($first);

        $merged = $sinks->merge(new Sinks($second));

        static::assertSame([$first, $second], $merged->all());
        static::assertSame([$first], $sinks->all());
    }
}
