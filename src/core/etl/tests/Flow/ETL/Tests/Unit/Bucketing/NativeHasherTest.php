<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

final class NativeHasherTest extends FlowTestCase
{
    public function test_empty_batch_returns_empty_list(): void
    {
        static::assertSame([], (new NativeHasher())->hash([]));
    }

    public function test_hash_differs_for_different_values(): void
    {
        $hashes = (new NativeHasher())->hash([['PL'], ['US']]);

        static::assertNotSame($hashes[0], $hashes[1]);
    }

    public function test_hash_handles_null_values(): void
    {
        static::assertCount(1, (new NativeHasher())->hash([[null]]));
        static::assertSame((new NativeHasher())->hash([[null]]), (new NativeHasher())->hash([[null]]));
    }

    public function test_hash_is_deterministic_for_equal_values(): void
    {
        static::assertSame((new NativeHasher())->hash([[1, 'PL']]), (new NativeHasher())->hash([[1, 'PL']]));
    }

    public function test_hash_returns_one_hash_per_row(): void
    {
        static::assertCount(3, (new NativeHasher())->hash([[1], [2], [3]]));
    }

    public function test_boolean_does_not_collide_with_numeric(): void
    {
        static::assertNotSame((new NativeHasher())->hash([[true]]), (new NativeHasher())->hash([[1]]));
    }

    public function test_numeric_values_hash_equal_across_types(): void
    {
        static::assertSame((new NativeHasher())->hash([[1]]), (new NativeHasher())->hash([[1.0]]));
        static::assertSame((new NativeHasher())->hash([[1]]), (new NativeHasher())->hash([['1']]));
    }

    public function test_negative_zero_hashes_equal_to_zero(): void
    {
        static::assertSame((new NativeHasher())->hash([[-0.0]]), (new NativeHasher())->hash([[0.0]]));
    }

    public function test_datetime_instants_hash_equal_across_timezones(): void
    {
        static::assertSame(
            (new NativeHasher())->hash([[new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC'))]]),
            (new NativeHasher())->hash([[new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('+01:00'))]]),
        );
    }

    public function test_uuid_objects_hash_by_their_string_representation(): void
    {
        static::assertSame(
            (new NativeHasher())->hash([[new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]]),
            (new NativeHasher())->hash([[new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]]),
        );
        static::assertNotSame(
            (new NativeHasher())->hash([[new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]]),
            (new NativeHasher())->hash([[new Uuid('00000000-0000-4000-8000-000000000000')]]),
        );
    }

    public function test_normalize_distinguishes_null_from_string(): void
    {
        static::assertNotSame(NativeHasher::normalize(null), NativeHasher::normalize('null'));
    }
}
