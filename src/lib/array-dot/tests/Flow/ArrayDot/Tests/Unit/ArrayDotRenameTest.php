<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use function Flow\ArrayDot\array_dot_rename;
use PHPUnit\Framework\TestCase;

final class ArrayDotRenameTest extends TestCase
{
    public function test_renames_array_by_path() : void
    {
        $this->assertSame(
            [
                'users' => [
                    ['id' => 1, 'user_name' => 'John'],
                    ['id' => 2, 'name' => 'Paul'],
                ],
            ],
            array_dot_rename(
                [
                    'users' => [
                        ['id' => 1, 'name' => 'John'],
                        ['id' => 2, 'name' => 'Paul'],
                    ],
                ],
                'users.0.name',
                'user_name'
            )
        );
    }

    public function test_renames_array_by_path_with_asterix() : void
    {
        $this->assertSame(
            [
                'users' => [
                    ['id' => 1, 'user_name' => 'John'],
                    ['id' => 2, 'user_name' => 'Paul'],
                ],
            ],
            array_dot_rename(
                [
                    'users' => [
                        ['id' => 1, 'name' => 'John'],
                        ['id' => 2, 'name' => 'Paul'],
                    ],
                ],
                'users.*.name',
                'user_name'
            )
        );
    }

    public function test_renames_array_by_path_with_asterix_as_a_key() : void
    {
        $this->assertSame(
            [
                'users' => [
                    'john' => ['id' => 1],
                    'paul' => ['id' => 2],
                    '*' => ['asterix_id' => 3],
                ],
            ],
            array_dot_rename(
                [
                    'users' => [
                        'john' => ['id' => 1],
                        'paul' => ['id' => 2],
                        '*' => ['id' => 3],
                    ],
                ],
                'users.\\*.id',
                'asterix_id'
            )
        );
    }

    public function test_renames_array_by_path_with_multiple_asterix() : void
    {
        $this->assertSame(
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
                        'id' => 2,
                        'packages' => [
                            [
                                'label_id' => '3333',
                            ],
                        ],
                    ],
                ],
            ],
            array_dot_rename(
                [
                    'transactions' => [
                        [
                            'id' => 1,
                            'packages' => [
                                [
                                    'id' => '12345',
                                ],
                                [
                                    'id' => '22222',
                                ],
                            ],
                        ],
                        [
                            'id' => 2,
                            'packages' => [
                                [
                                    'id' => '3333',
                                ],
                            ],
                        ],
                    ],
                ],
                'transactions.*.packages.*.id',
                'label_id'
            ),
        );
    }

    public function test_renames_array_root_key_name() : void
    {
        $this->assertEquals(
            [
                'admins' => [
                    ['id' => 1, 'name' => 'John'],
                    ['id' => 2, 'date' => 'Paul'],
                ],
                'status' => 'active',
            ],
            array_dot_rename(
                [
                    'users' => [
                        ['id' => 1, 'name' => 'John'],
                        ['id' => 2, 'date' => 'Paul'],
                    ],
                    'status' => 'active',
                ],
                'users',
                'admins'
            )
        );
    }
}
