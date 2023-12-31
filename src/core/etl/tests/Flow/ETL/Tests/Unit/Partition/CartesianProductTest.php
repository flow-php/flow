<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Partition;

use Flow\ETL\Partition\CartesianProduct;
use PHPUnit\Framework\TestCase;

final class CartesianProductTest extends TestCase
{
    public function test_cartesian_product_with_associative_arrays() : void
    {
        $input = ['first' => [1, 2], 'second' => ['a', 'b']];
        $expected = [['first' => 1, 'second' => 'a'], ['first' => 1, 'second' => 'b'], ['first' => 2, 'second' => 'a'], ['first' => 2, 'second' => 'b']];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }

    public function test_cartesian_product_with_different_lengths() : void
    {
        $input = [[1], ['a', 'b'], [true, false]];
        $expected = [[1, 'a', true], [1, 'a', false], [1, 'b', true], [1, 'b', false]];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }

    public function test_cartesian_product_with_edge_cases() : void
    {
        $input = [[null, 2], [0]];
        $expected = [[null, 0], [2, 0]];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }

    public function test_cartesian_product_with_empty_input() : void
    {
        $input = [];
        $expected = [[]];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }

    public function test_cartesian_product_with_multiple_arrays() : void
    {
        $input = [[1, 2], ['a', 'b']];
        $expected = [[1, 'a'], [1, 'b'], [2, 'a'], [2, 'b']];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }

    public function test_cartesian_product_with_one_array() : void
    {
        $input = [[1, 2]];
        $expected = [[1], [2]];
        $result = (new CartesianProduct)($input);
        $this->assertEquals($expected, $result);
    }
}
