<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Tests\FlowTestCase;

final class NativeHasherTest extends FlowTestCase
{
    public function test_empty_batch_returns_empty_list(): void
    {
        static::assertSame([], (new NativeHasher())->hash([]));
    }

    public function test_hash_differs_for_different_values(): void
    {
        $hashes = (new NativeHasher())->hash([['country' => 'PL'], ['country' => 'US']]);

        static::assertNotSame($hashes[0], $hashes[1]);
    }

    public function test_hash_handles_null_values(): void
    {
        static::assertCount(1, (new NativeHasher())->hash([['id' => null]]));
    }

    public function test_hash_is_deterministic_for_equal_values(): void
    {
        static::assertSame(
            (new NativeHasher())->hash([['id' => 1, 'country' => 'PL']]),
            (new NativeHasher())->hash([['id' => 1, 'country' => 'PL']]),
        );
    }

    public function test_hash_returns_one_hash_per_row(): void
    {
        static::assertCount(3, (new NativeHasher())->hash([['id' => 1], ['id' => 2], ['id' => 3]]));
    }

    public function test_normalize_distinguishes_null_from_string(): void
    {
        static::assertNotSame(NativeHasher::normalize(null), NativeHasher::normalize('null'));
    }
}
