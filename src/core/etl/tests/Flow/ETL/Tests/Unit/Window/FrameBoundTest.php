<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\FrameBound;
use Flow\ETL\Window\FrameBoundType;

use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\unbounded_following;
use function Flow\ETL\DSL\unbounded_preceding;

final class FrameBoundTest extends FlowTestCase
{
    public function test_current_row_resolves_to_the_current_index(): void
    {
        static::assertSame(3, current_row()->resolve(3, 9));
    }

    public function test_current_row_with_an_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound of type CURRENT_ROW must not define an offset.');

        new FrameBound(FrameBoundType::CURRENT_ROW, 1);
    }

    public function test_dsl_functions_build_the_expected_bounds(): void
    {
        static::assertSame(FrameBoundType::UNBOUNDED_PRECEDING, unbounded_preceding()->type);
        static::assertNull(unbounded_preceding()->offset);

        static::assertSame(FrameBoundType::PRECEDING, preceding(2)->type);
        static::assertSame(2, preceding(2)->offset);

        static::assertSame(FrameBoundType::CURRENT_ROW, current_row()->type);
        static::assertNull(current_row()->offset);

        static::assertSame(FrameBoundType::FOLLOWING, following(3)->type);
        static::assertSame(3, following(3)->offset);

        static::assertSame(FrameBoundType::UNBOUNDED_FOLLOWING, unbounded_following()->type);
        static::assertNull(unbounded_following()->offset);
    }

    public function test_following_resolves_forward_from_the_current_index(): void
    {
        static::assertSame(5, following(2)->resolve(3, 9));
    }

    public function test_following_without_an_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound of type FOLLOWING requires an offset.');

        new FrameBound(FrameBoundType::FOLLOWING);
    }

    public function test_negative_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound offset must not be negative, got -1.');

        new FrameBound(FrameBoundType::PRECEDING, -1);
    }

    public function test_preceding_resolves_backwards_from_the_current_index(): void
    {
        static::assertSame(1, preceding(2)->resolve(3, 9));
    }

    public function test_preceding_resolves_below_zero_without_clamping(): void
    {
        static::assertSame(-2, preceding(2)->resolve(0, 9));
    }

    public function test_preceding_without_an_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound of type PRECEDING requires an offset.');

        new FrameBound(FrameBoundType::PRECEDING);
    }

    public function test_unbounded_following_resolves_to_the_last_index(): void
    {
        static::assertSame(9, unbounded_following()->resolve(3, 9));
    }

    public function test_unbounded_following_with_an_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound of type UNBOUNDED_FOLLOWING must not define an offset.');

        new FrameBound(FrameBoundType::UNBOUNDED_FOLLOWING, 5);
    }

    public function test_unbounded_preceding_resolves_to_zero(): void
    {
        static::assertSame(0, unbounded_preceding()->resolve(3, 9));
    }

    public function test_unbounded_preceding_with_an_offset_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Frame bound of type UNBOUNDED_PRECEDING must not define an offset.');

        new FrameBound(FrameBoundType::UNBOUNDED_PRECEDING, 0);
    }

    public function test_zero_offset_is_allowed(): void
    {
        static::assertSame(0, preceding(0)->offset);
        static::assertSame(3, preceding(0)->resolve(3, 9));
    }
}
