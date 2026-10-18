<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter;

use Flow\Parquet\Filter\Truth;
use Generator;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class TruthTest extends TestCase
{
    public function test_every_is_never_when_one_is_never_and_always_only_when_all_are(): void
    {
        static::assertSame(Truth::Always, Truth::every([]));
        static::assertSame(Truth::Always, Truth::every([Truth::Always, Truth::Always]));
        static::assertSame(Truth::Maybe, Truth::every([Truth::Always, Truth::Maybe]));
        static::assertSame(Truth::Never, Truth::every([Truth::Maybe, Truth::Never]));
    }

    public function test_some_is_always_when_one_is_always_and_never_only_when_all_are(): void
    {
        static::assertSame(Truth::Never, Truth::some([]));
        static::assertSame(Truth::Never, Truth::some([Truth::Never, Truth::Never]));
        static::assertSame(Truth::Maybe, Truth::some([Truth::Never, Truth::Maybe]));
        static::assertSame(Truth::Always, Truth::some([Truth::Maybe, Truth::Always]));
    }

    public function test_both_stop_at_the_deciding_truth(): void
    {
        $never = (
            /** @return Generator<int, Truth> */
            static function (): Generator {
                yield Truth::Maybe;
                yield Truth::Never;

                throw new RuntimeException('evaluated past the deciding truth');
            }
        )();
        $always = (
            /** @return Generator<int, Truth> */
            static function (): Generator {
                yield Truth::Maybe;
                yield Truth::Always;

                throw new RuntimeException('evaluated past the deciding truth');
            }
        )();

        static::assertSame(Truth::Never, Truth::every($never));
        static::assertSame(Truth::Always, Truth::some($always));
    }
}
