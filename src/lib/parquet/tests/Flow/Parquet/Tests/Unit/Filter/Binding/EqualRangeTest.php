<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Binding;

use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\OutOfDomain;
use PHPUnit\Framework\TestCase;

final class EqualRangeTest extends TestCase
{
    public function test_a_point_is_its_own_low_and_high(): void
    {
        static::assertEquals(new EqualRange('a', 'a'), EqualRange::point('a'));
        static::assertEquals(
            new EqualRange(OutOfDomain::Above, OutOfDomain::Above),
            EqualRange::point(OutOfDomain::Above),
        );
    }

    public function test_an_integer_edge_beyond_the_domain_is_out_of_it(): void
    {
        static::assertEquals(new EqualRange(1, 2), EqualRange::integers('1', '2', 0, 10));
        static::assertEquals(new EqualRange(OutOfDomain::Below, 10), EqualRange::integers('-1', '10', 0, 10));
        static::assertEquals(new EqualRange(0, OutOfDomain::Above), EqualRange::integers('0', '11', 0, 10));
    }

    public function test_a_value_on_the_grid_is_one_physical_value_and_one_off_it_none(): void
    {
        static::assertEquals(new EqualRange(2, 2), EqualRange::grid('2000', '1000', -10, 10));
        static::assertEquals(new EqualRange(2, 1), EqualRange::grid('1500', '1000', -10, 10));
        static::assertEquals(new EqualRange(-1, -2), EqualRange::grid('-1500', '1000', -10, 10));
        static::assertEquals(new EqualRange(OutOfDomain::Above, 10), EqualRange::grid('10500', '1000', -10, 10));
    }
}
