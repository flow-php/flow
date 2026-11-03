<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\for_each;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class ForEachRowsTest extends FlowTestCase
{
    public function test_for_each_hands_every_batch_to_the_callback(): void
    {
        $batches = [];

        for_each(static function (Rows $rows) use (&$batches): void {
            $batches[] = $rows->toArray();
        })(df()->read(from_array([['a' => 1], ['a' => 2]]))->batchSize(1));

        static::assertSame([[['a' => 1]], [['a' => 2]]], $batches);
    }

    public function test_for_each_without_a_callback_runs_the_frame(): void
    {
        $memory = new ArrayMemory();

        for_each()(df()->read(from_array([['a' => 1], ['a' => 2]]))->write(to_memory($memory)));

        static::assertSame([['a' => 1], ['a' => 2]], $memory->dump());
    }
}
