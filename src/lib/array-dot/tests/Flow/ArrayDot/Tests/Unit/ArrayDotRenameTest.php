<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ArrayDot\array_dot_rename;

final class ArrayDotRenameTest extends TestCase
{
    public function test_renames_array_by_path(): void
    {
        static::assertSame(
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
                'user_name',
            ),
        );
    }

    public function test_renames_array_by_path_with_asterix(): void
    {
        static::assertSame(
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
                'user_name',
            ),
        );
    }

    public function test_renames_array_by_path_with_asterix_as_a_key(): void
    {
        static::assertSame(
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
                'asterix_id',
            ),
        );
    }

    public function test_renames_array_by_path_with_multiple_asterix(): void
    {
        static::assertSame(
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
                'label_id',
            ),
        );
    }

    public function test_renames_array_root_key_name(): void
    {
        static::assertEquals(
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
                'admins',
            ),
        );
    }

    public function test_renames_a_key_written_with_escaped_braces(): void
    {
        static::assertSame(['b' => 1], array_dot_rename(['{a}' => 1], '\\{a\\}', 'b'));
    }

    public function test_renames_a_dotted_key_after_a_wildcard(): void
    {
        static::assertSame(['x' => [['c' => 1]]], array_dot_rename(['x' => [['a.b' => 1]]], 'x.*.a\\.b', 'c'));
    }

    public function test_rename_of_an_absent_nullsafe_key_changes_nothing(): void
    {
        static::assertSame(['a' => 1], array_dot_rename(['a' => 1], '?b', 'c'));
    }

    public function test_rename_skips_elements_without_the_key_under_a_nullsafe_wildcard(): void
    {
        static::assertSame(
            ['x' => [['k' => 1], ['m' => 2]]],
            array_dot_rename(['x' => [['n' => 1], ['m' => 2]]], 'x.?*.n', 'k'),
        );
    }

    #[TestWith(['x.*'])]
    #[TestWith(['x.{a,b}'])]
    public function test_rename_refuses_a_path_not_ending_with_a_key(string $path): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('does not end with a key');

        array_dot_rename(['x' => ['a' => 1, 'b' => 2]], $path, 'c');
    }

    public function test_rename_through_a_path_object(): void
    {
        static::assertSame(['c' => 1], array_dot_rename(['a.b' => 1], new Path([new Key('a.b')]), 'c'));
    }

    public function test_rename_of_a_missing_path_fails(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "b" does not exists in array');

        array_dot_rename(['a' => 1], 'b', 'c');
    }

    /**
     * @param array<mixed> $array
     */
    #[TestWith([['a' => 1, 'b' => 2], 'a', 'a'])]
    #[TestWith([[5 => 'v'], '5', '5'])]
    #[TestWith([['x' => [['n' => 1], ['n' => 2]]], 'x.*.n', 'n'])]
    public function test_renaming_a_key_to_its_own_name_changes_nothing(
        array $array,
        string $path,
        string $newName,
    ): void {
        static::assertSame($array, array_dot_rename($array, $path, $newName));
    }
}
