<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use Flow\ETL\Column\Elements;
use PHPUnit\Framework\TestCase;

final class ElementsTest extends TestCase
{
    public function test_flatten_takes_every_element_of_every_non_null_container_in_order(): void
    {
        static::assertSame(
            [1, null, 2, 3, [4]],
            (new Elements())->flatten([[1, null], null, [], ['a' => 2, 5 => 3], [[4]]]),
        );
        static::assertSame([], (new Elements())->flatten([]));
    }

    public function test_shape_cuts_a_flat_list_back_into_the_containers(): void
    {
        static::assertSame(
            [['a', 'b'], null, [], ['a' => 'c', 5 => 'd']],
            (new Elements())->shape(['a', 'b', 'c', 'd'], [[1, null], null, [], ['a' => 2, 5 => 3]]),
        );
    }

    public function test_field_takes_the_element_of_every_structure_that_has_it(): void
    {
        static::assertSame(
            [1, null],
            (new Elements())->field([['a' => 1, 'b' => 2], ['b' => 3], null, ['a' => null]], 'a'),
        );
        static::assertSame(['x'], (new Elements())->field([[0 => 'x']], 0));
    }

    public function test_with_field_puts_rendered_values_back_where_the_field_was(): void
    {
        static::assertSame(
            [['a' => 'one', 'b' => 2], ['b' => 3], null, ['a' => 'two']],
            (new Elements())->withField([['a' => 1, 'b' => 2], ['b' => 3], null, ['a' => null]], 'a', ['one', 'two']),
        );
    }
}
