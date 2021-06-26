<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidPathException;
use PHPUnit\Framework\TestCase;

final class ArrayDotTest extends TestCase
{
    public function test_accessing_array_value_by_path() : void
    {
        $this->assertSame(
            1,
            array_dot_get(
                [
                    'user' => [
                        'id' => 1,
                    ],
                ],
                'user.id'
            )
        );
        $this->assertTrue(
            array_dot_exists(
                [
                    'user' => [
                        'id' => 1,
                    ],
                ],
                'user.id'
            )
        );
        $this->assertFalse(
            array_dot_exists(
                [
                    'user' => [
                        'id' => 1,
                    ],
                ],
                'invalid_path'
            )
        );
    }

    public function test_escape_dot_path() : void
    {
        $this->assertSame(
            'baz',
            array_dot_get(
                [
                    'user' => [
                        'id' => 1,
                        'foo.bar' => 'baz',
                    ],
                ],
                'user.foo\\.bar'
            ),
        );
    }

    public function test_accessing_array_value_by_nullsafe_path() : void
    {
        $this->assertNull(
            array_dot_get(
                [
                    'user' => [
                        'id' => 1,
                    ],
                ],
                'user.?name'
            )
        );
        $this->assertNull(
            array_dot_get(
                [
                    'user' => [
                        'role' => [
                            'name' => 'admin',
                        ],
                    ],
                ],
                'user.?wrong_path.name'
            )
        );
        $this->assertNull(
            array_dot_get(
                [
                    'users' => [],
                ],
                'users.?0.name'
            )
        );
    }

    public function test_accessing_nested_array_value_by_numeric_path() : void
    {
        $this->assertSame(
            1,
            array_dot_get(
                [
                    'users' => [
                        [
                            'user' => [
                                'id' => 1,
                            ],
                        ],
                    ],
                ],
                'users.0.user.id'
            )
        );
    }

    public function test_accessing_array_value_by_invalid_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage("Path \"invalid_path\" does not exists in array \"array('user'=>array('id'=>1,),)\"");

        $this->assertSame(array_dot_get(['user' => ['id' => 1]], 'invalid_path'), 1);
    }

    public function test_accessing_empty_array_value_by_invalid_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array "array()"');

        $this->assertSame(array_dot_get([], 'invalid_path'), 1);
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix() : void
    {
        $this->assertSame(
            ['Michael', 'Jack'],
            array_dot_get(
                [
                    'users' => [
                        [
                            'user' => [
                                'id' => 1,
                                'name' => 'Michael',
                            ],
                        ],
                        [
                            'user' => [
                                'id' => 2,
                                'name' => 'Jack',
                            ],
                        ],
                    ],
                ],
                'users.*.user.name'
            ),
        );
    }

    public function test_accessing_array_value_by_path_with_asterix() : void
    {
        $this->assertSame(
            [
                [
                    'id' => 1,
                    'name' => 'Michael',
                ],
                [
                    'id' => 2,
                    'name' => 'Jack',
                ],
            ],
            array_dot_get(
                [
                    'users' => [
                        [
                            'user' => [
                                'id' => 1,
                                'name' => 'Michael',
                            ],
                        ],
                        [
                            'user' => [
                                'id' => 2,
                                'name' => 'Jack',
                            ],
                        ],
                    ],
                ],
                'users.*.user'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage("Path \"user.name\" does not exists in array \"array('user'=>array('id'=>2,),)\"");

        array_dot_get(
            [
                'users' => [
                    [
                        'user' => [
                            'id' => 1,
                            'name' => 'Michael',
                        ],
                    ],
                    [
                        'user' => [
                            'id' => 2,
                        ],
                    ],
                ],
            ],
            'users.*.user.name'
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_asterix_key() : void
    {
        $this->assertSame(
            'Michael',
            array_dot_get(
                [
                    'users' => [
                        '*' => [
                            'id' => 1,
                            'name' => 'Michael',
                        ],
                    ],
                ],
                'users.\\*.name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_nullable_asterix_and_different_elements() : void
    {
        $this->assertSame(
            ['Michael'],
            array_dot_get(
                [
                    'users' => [
                        [
                            'user' => [
                                'id' => 1,
                                'name' => 'Michael',
                            ],
                        ],
                        [
                            'user' => [
                                'id' => 2,
                            ],
                        ],
                    ],
                ],
                'users.?*.user.name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements_using_nullsafe() : void
    {
        $this->assertSame(
            ['Michael', null],
            array_dot_get(
                [
                    'users' => [
                        [
                            'user' => [
                                'id' => 1,
                                'name' => 'Michael',
                            ],
                        ],
                        [
                            'user' => [
                                'id' => 2,
                            ],
                        ],
                    ],
                ],
                'users.*.user.?name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_nullable_asterix() : void
    {
        $this->assertSame(
            'Michael',
            array_dot_get(
                [
                    'users' => [
                        '?*' => [
                            'id' => 1, 'name' => 'Michael',
                        ],
                    ],
                ],
                'users.\\?*.name'
            )
        );
    }

    public function test_accessing_array_scalar_value_by_path_multiple_asterix_paths() : void
    {
        $this->assertSame(
            [
                ['12345', '22222'],
                ['3333'],
            ],
            array_dot_get(
                [
                    'transactions' => [
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'label_id' => '12345',
                                ],
                                [
                                    'label_id' => '22222', ],
                            ],
                        ],
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'label_id' => '3333',
                                ],
                            ],
                        ],
                    ],
                ],
                'transactions.*.packages.*.label_id'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_multiple_asterix_paths_with_nullsafe() : void
    {
        $this->assertSame(
            [
                ['12345', '22222'],
                ['3333'],
                [null],
            ],
            array_dot_get(
                [
                    'transactions' => [
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'label_id' => '12345',
                                ],
                                [
                                    'label_id' => '22222',
                                ],
                            ],
                        ],
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'label_id' => '3333',
                                ],
                            ],
                        ],
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'foo' => 'bar',
                                ],
                            ],
                        ],
                    ],
                ],
                'transactions.*.packages.*.?label_id'
            ),
        );
    }

    public function test_single_multi_key_get() : void
    {
        $this->assertSame(
            [
                1, 'foo',
            ],
            array_dot_get(
                [
                    'array' => [
                        [
                            'id' => 1,
                            'name' => 'foo',
                        ],
                        [
                            'id' => 2,
                            'name' => 'bar',
                        ],
                        [
                            'id' => 3,
                            'name' => 'baz',
                        ],
                    ],
                ],
                'array.0.{id,name}'
            ),
        );
    }

    public function test_escape_multi_key_syntax() : void
    {
        $this->assertSame(
            1,
            array_dot_get(
                [
                    'array' => [
                        '{id}' => 1,
                    ],
                ],
                'array.\\{id\\}'
            ),
        );
    }

    public function test_single_nullsafe_multi_key_get() : void
    {
        $this->assertSame(
            [
                1, null,
            ],
            array_dot_get(
                [
                    'array' => [
                        [
                            'id' => 1,
                        ],
                        [
                            'id' => 2,
                            'name' => 'bar',
                        ],
                        [
                            'id' => 3,
                            'name' => 'baz',
                        ],
                    ],
                ],
                'array.0.{id,?name}'
            ),
        );
    }

    public function test_all_multi_key_get() : void
    {
        $this->assertSame(
            [
                [1, 'foo'],
                [2, 'bar'],
                [3, 'baz'],
            ],
            array_dot_get(
                [
                    'array' => [
                        [
                            'id' => 1,
                            'name' => 'foo',
                        ],
                        [
                            'id' => 2,
                            'name' => 'bar',
                        ],
                        [
                            'id' => 3,
                            'name' => 'baz',
                        ],
                    ],
                ],
                'array.*.{id, name}'
            ),
        );
    }

    public function test_all_multi_key_get_nested() : void
    {
        $this->assertSame(
            [
                [1, 'foo', 'active'],
                [2, 'bar', 'active'],
                [3, 'baz', 'disabled'],
            ],
            array_dot_get(
                [
                    'array' => [
                        [
                            'id' => 1,
                            'name' => 'foo',
                            'property' => ['status' => 'active'],
                        ],
                        [
                            'id' => 2,
                            'name' => 'bar',
                            'property' => ['status' => 'active'],
                        ],
                        [
                            'id' => 3,
                            'name' => 'baz',
                            'property' => ['status' => 'disabled'],
                        ],
                    ],
                ],
                'array.*.{id, name,    property.status}'
            ),
        );
    }
}
