<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use Flow\ETL\Kernels\Bucketing\SeenKeys;
use Flow\ETL\Tests\FlowTestCase;

use function str_repeat;

final class SeenKeysTest extends FlowTestCase
{
    public function test_add_answers_the_first_row_of_each_new_key_ascending(): void
    {
        static::assertSame([0, 1, 3], (new SeenKeys())->add(['a', 'b', 'a', 'c', 'b']));
    }

    public function test_a_key_seen_in_an_earlier_batch_is_not_new(): void
    {
        $seen = new SeenKeys();
        $seen->add(['a', 'b']);

        static::assertSame([1], $seen->add(['b', 'c']));
    }

    public function test_count_is_the_number_of_distinct_keys(): void
    {
        $seen = new SeenKeys();
        $seen->add(['a', 'b', 'a']);
        $seen->add([str_repeat('x', 10_000), 'b']);

        static::assertSame(3, $seen->count());
    }

    public function test_no_keys_add_nothing(): void
    {
        $seen = new SeenKeys();

        static::assertSame([], $seen->add([]));
        static::assertSame(0, $seen->count());
    }
}
