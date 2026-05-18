<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use DateTimeImmutable;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Tests\Unit\Fixtures\Letters;
use Flow\ArrayDot\Tests\Unit\Fixtures\Numbers;
use PHPUnit\Framework\TestCase;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class ArrayDotGetTest extends TestCase
{
    public function test_accessing_array_scalar_value_by_path_multiple_asterix_paths(): void
    {
        static::assertSame(
            [
                ['12345', '22222'],
                ['3333'],
            ],
            array_dot_get([
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
                ],
            ], 'transactions.*.packages.*.label_id'),
        );
    }

    public function test_accessing_array_scalar_value_by_path_multiple_asterix_paths_with_nullsafe(): void
    {
        static::assertSame(
            [
                ['12345', '22222'],
                ['3333'],
                [null],
            ],
            array_dot_get([
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
            ], 'transactions.*.packages.*.?label_id'),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix(): void
    {
        static::assertSame(
            ['Michael', 'Jack'],
            array_dot_get([
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
            ], 'users.*.user.name'),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage(
            "Path \"user.name\" does not exists in array \"array('user'=>array('id'=>2,),)\"",
        );

        array_dot_get([
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
        ], 'users.*.user.name');
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements_using_nullsafe(): void
    {
        static::assertSame(
            ['Michael', null],
            array_dot_get([
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
            ], 'users.*.user.?name'),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_nullable_asterix(): void
    {
        static::assertSame('Michael', array_dot_get([
            'users' => [
                '?*' => [
                    'id' => 1,
                    'name' => 'Michael',
                ],
            ],
        ], 'users.\\?*.name'));
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_wildcard_key(): void
    {
        static::assertSame('Michael', array_dot_get([
            'users' => [
                '*' => [
                    'id' => 1,
                    'name' => 'Michael',
                ],
            ],
        ], 'users.\\*.name'));
    }

    public function test_accessing_array_scalar_value_by_path_with_nullable_asterix_and_different_elements(): void
    {
        static::assertSame(
            ['Michael'],
            array_dot_get([
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
            ], 'users.?*.user.name'),
        );
    }

    public function test_accessing_array_value_by_invalid_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage(
            "Path \"invalid_path\" does not exists in array \"array('user'=>array('id'=>1,),)\"",
        );

        static::assertSame(array_dot_get(['user' => ['id' => 1]], 'invalid_path'), 1);
    }

    public function test_accessing_array_value_by_nullsafe_path(): void
    {
        static::assertNull(array_dot_get([
            'user' => [
                'id' => 1,
            ],
        ], 'user.?name'));
        static::assertNull(array_dot_get([
            'user' => [
                'role' => [
                    'name' => 'admin',
                ],
            ],
        ], 'user.?wrong_path.name'));
        static::assertNull(array_dot_get([
            'users' => [],
        ], 'users.?0.name'));
    }

    public function test_accessing_array_value_by_path(): void
    {
        static::assertSame(1, array_dot_get([
            'user' => [
                'id' => 1,
            ],
        ], 'user.id'));
        static::assertTrue(array_dot_exists([
            'user' => [
                'id' => 1,
            ],
        ], 'user.id'));
        static::assertFalse(array_dot_exists([
            'user' => [
                'id' => 1,
            ],
        ], 'invalid_path'));
    }

    public function test_accessing_array_value_by_path_with_asterix(): void
    {
        static::assertSame(
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
            array_dot_get([
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
            ], 'users.*.user'),
        );
    }

    public function test_accessing_empty_array_value_by_invalid_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array "array()"');

        static::assertSame(array_dot_get([], 'invalid_path'), 1);
    }

    public function test_accessing_nested_array_value_by_numeric_path(): void
    {
        static::assertSame(1, array_dot_get([
            'users' => [
                [
                    'user' => [
                        'id' => 1,
                    ],
                ],
            ],
        ], 'users.0.user.id'));
    }

    public function test_accessing_nested_collection_using_wildcard(): void
    {
        static::assertSame(
            [
                [
                    'id' => 1,
                    'name' => 'Michael',
                ],
                [
                    'id' => 2,
                    'name' => 'Rocky',
                ],
            ],
            array_dot_get([
                'users' => [
                    [
                        'id' => 1,
                        'name' => 'Michael',
                    ],
                    [
                        'id' => 2,
                        'name' => 'Rocky',
                    ],
                ],
            ], 'users.*'),
        );
    }

    public function test_accessing_not_nested_nullsafe(): void
    {
        static::assertNull(array_dot_get([
            '@name' => 'Test',
        ], '?@id'));
    }

    public function test_accessing_not_nested_nullsafe_on_empty_array(): void
    {
        static::assertNull(array_dot_get([], '?@id'));
    }

    public function test_accessing_null_value_under_existing_path(): void
    {
        static::assertTrue(array_dot_exists([
            'user' => [
                'id' => null,
            ],
        ], 'user.id'));
        static::assertFalse(array_dot_exists([
            'user' => [
                'id' => null,
            ],
        ], 'user.ids'));
        static::assertTrue(array_dot_exists([
            'user' => [
                'id' => null,
            ],
        ], '?user?.ids'));
    }

    public function test_all_multi_key_get(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'name' => 'foo'],
                ['id' => 2, 'name' => 'bar'],
                ['id' => 3, 'name' => 'baz'],
            ],
            array_dot_get([
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
            ], 'array.*.{id, name}'),
        );
    }

    public function test_all_multi_key_get_nested(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'name' => 'foo', 'property_status_value' => 'active'],
                ['id' => 2, 'name' => 'bar', 'property_status_value' => 'active'],
                ['id' => 3, 'name' => 'baz', 'property_status_value' => 'disabled'],
            ],
            array_dot_get([
                'array' => [
                    [
                        'id' => 1,
                        'name' => 'foo',
                        'property' => ['status' => ['value' => 'active']],
                    ],
                    [
                        'id' => 2,
                        'name' => 'bar',
                        'property' => ['status' => ['value' => 'active']],
                    ],
                    [
                        'id' => 3,
                        'name' => 'baz',
                        'property' => ['status' => ['value' => 'disabled']],
                    ],
                ],
            ], 'array.*.{id, name,    property.?status.value}'),
        );
    }

    public function test_array_dot_get_boolean(): void
    {
        static::assertTrue(array_dot_get(['active' => true], 'active', type_boolean()));

        static::assertFalse(array_dot_get(['active' => false], 'active', type_boolean()));

        static::assertNull(array_dot_get(['activate' => 'true'], '?active', type_boolean()));
    }

    public function test_array_dot_get_datetime(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2021-01-01 00:00:00'),
            array_dot_get(['created_at' => '2021-01-01 00:00:00'], 'created_at', type_datetime()),
        );

        static::assertNull(array_dot_get(['created_at' => '2021-01-01 00:00:00'], '?updated_at', type_datetime()));
    }

    public function test_array_dot_get_enum(): void
    {
        $intValue = array_dot_get(['id' => 1], 'id', type_integer());
        static::assertSame(Numbers::ONE, $intValue === null ? null : Numbers::tryFrom($intValue));

        $stringValue = array_dot_get(['id' => 'A'], 'id', type_string());
        static::assertSame(Letters::A, $stringValue === null ? null : Letters::tryFrom($stringValue));

        $coercedValue = array_dot_get(['id' => '1'], 'id', type_integer());
        static::assertSame(Numbers::ONE, $coercedValue === null ? null : Numbers::tryFrom($coercedValue));

        $missingValue = array_dot_get(['identifier' => 1], '?id', type_integer());
        static::assertNull($missingValue === null ? null : Numbers::tryFrom($missingValue));
    }

    public function test_array_dot_get_float(): void
    {
        static::assertSame(1.0, array_dot_get(['id' => 1.0], 'id', type_float()));

        static::assertSame(10.0, array_dot_get(['id' => 10], 'id', type_float()));

        static::assertSame(1.0, array_dot_get(['id' => '1.0'], 'id', type_float()));

        static::assertNull(array_dot_get(['identifier' => 1.0], '?id', type_float()));
    }

    public function test_array_dot_get_int(): void
    {
        static::assertSame(1, array_dot_get(['id' => 1], 'id', type_integer()));

        static::assertSame(1, array_dot_get(['id' => '01'], 'id', type_integer()));

        static::assertNull(array_dot_get(['identifier' => 1], '?id', type_integer()));
    }

    public function test_array_dot_get_string(): void
    {
        static::assertSame('foo', array_dot_get(['name' => 'foo'], 'name', type_string()));

        static::assertSame('1', array_dot_get(['name' => 1], 'name', type_string()));

        static::assertNull(array_dot_get(['identifier' => 'foo'], '?name', type_string()));
    }

    public function test_escape_dot_path(): void
    {
        static::assertSame('baz', array_dot_get([
            'user' => [
                'id' => 1,
                'foo.bar' => 'baz',
            ],
        ], 'user.foo\\.bar'));
    }

    public function test_escape_multi_key_syntax(): void
    {
        static::assertSame(1, array_dot_get([
            'array' => [
                '{id}' => 1,
            ],
        ], 'array.\\{id\\}'));
    }

    public function test_get_nullsafe_wildcard_from_not_array(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Expected array under path, "?*", but got: integer');

        array_dot_get([
            'id' => 1,
            'status' => 'NEW',
        ], '?*.{id}');
    }

    public function test_get_wildcard_from_not_array(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Expected array under path, "*", but got: integer');

        array_dot_get([
            'id' => 1,
            'status' => 'NEW',
        ], '*.{id}');
    }

    public function test_get_traversing_past_scalar_leaf(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Expected array under path, "user", but got: integer');

        array_dot_get(['user' => 1], 'user.name');
    }

    public function test_get_traversing_past_scalar_leaf_with_nullsafe_next_step(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Expected array under path, "user", but got: string');

        array_dot_get(['user' => 'admin'], 'user.?role');
    }

    public function test_get_traversing_past_null_leaf(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Expected array under path, "user", but got: NULL');

        array_dot_get(['user' => null], 'user.name');
    }

    public function test_single_multi_key_get(): void
    {
        static::assertSame(
            [
                'id' => 1,
                'name' => 'foo',
            ],
            array_dot_get([
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
            ], 'array.0.{id,name}'),
        );
    }

    public function test_single_nullsafe_multi_key_get(): void
    {
        static::assertSame(
            [
                'id' => 1,
                'name' => null,
            ],
            array_dot_get([
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
            ], 'array.0.{id,?name}'),
        );
    }
}
