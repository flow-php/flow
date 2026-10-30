<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical;

use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;
use function iterator_to_array;

final class WritesTest extends FlowTestCase
{
    public function test_an_empty_collection_holds_nothing(): void
    {
        $writes = new Writes();

        static::assertSame([], $writes->all());
        static::assertSame([], iterator_to_array($writes));
    }

    public function test_it_keeps_the_writes_in_the_order_given(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $writes = new Writes($write, $transaction);

        static::assertSame([$write, $transaction], $writes->all());
        static::assertSame([$write, $transaction], iterator_to_array($writes));
    }

    public function test_merge_appends_the_given_writes_after_its_own(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));
        $writes = new Writes($first);

        static::assertSame([$first, $second], $writes->merge(new Writes($second))->all());
        static::assertSame([$first], $writes->all());
    }
}
