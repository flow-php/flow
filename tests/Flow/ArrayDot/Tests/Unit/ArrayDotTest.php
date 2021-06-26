<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ArrayDotTest extends TestCase
{
    public function test_accessing_array_value_by_path() : void
    {
        $this->assertSame(array_dot_get(['user' => ['id' => 1]], 'user.id'), 1);
        $this->assertTrue(array_dot_exists(['user' => ['id' => 1]], 'user.id'));
        $this->assertFalse(array_dot_exists(['user' => ['id' => 1]], 'invalid_path'));
    }

    public function test_escaping_dot_path() : void
    {
        $this->assertSame(array_dot_get(['user' => ['id' => 1, 'foo.bar' => 'baz']], 'user.foo\\.bar'), 'baz');
    }

    public function test_accessing_array_value_by_nullsafe_path() : void
    {
        $this->assertNull(array_dot_get(['user' => ['id' => 1]], 'user.?name'));
        $this->assertNull(array_dot_get(['user' => ['role' => ['name' => 'admin']]], 'user.?wrong_path.name'));
        $this->assertNull(array_dot_get(['users' => []], 'users.?0.name'));
    }

    public function test_accessing_nested_array_value_by_numeric_path() : void
    {
        $this->assertSame(array_dot_get(['users' => [['user' => ['id' => 1]]]], 'users.0.user.id'), 1);
    }

    public function test_accessing_array_value_by_invalid_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Path \"invalid_path\" does not exists in array \"array('user'=>array('id'=>1,),)\"");

        $this->assertSame(array_dot_get(['user' => ['id' => 1]], 'invalid_path'), 1);
    }

    public function test_accessing_empty_array_value_by_invalid_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Path "invalid_path" does not exists in array "array()"');

        $this->assertSame(array_dot_get([], 'invalid_path'), 1);
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix() : void
    {
        $this->assertSame(
            ['Michael', 'Jack'],
            array_dot_get(
                ['users' => [['user' => ['id' => 1, 'name' => 'Michael']], ['user' => ['id' => 2, 'name' => 'Jack']]]],
                'users.*.user.name'
            ),
        );
    }

    public function test_accessing_array_value_by_path_with_asterix() : void
    {
        $this->assertSame(
            [['id' => 1, 'name' => 'Michael'], ['id' => 2, 'name' => 'Jack']],
            array_dot_get(
                ['users' => [['user' => ['id' => 1, 'name' => 'Michael']], ['user' => ['id' => 2, 'name' => 'Jack']]]],
                'users.*.user'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Path \"user.name\" does not exists in array \"array('user'=>array('id'=>2,),)\"");

        array_dot_get(
            ['users' => [['user' => ['id' => 1, 'name' => 'Michael']], ['user' => ['id' => 2]]]],
            'users.*.user.name'
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_asterix_key() : void
    {
        $this->assertSame(
            'Michael',
            array_dot_get(
                ['users' => ['*' => ['id' => 1, 'name' => 'Michael']]],
                'users.\\*.name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_nullable_asterix_and_different_elements() : void
    {
        $this->assertSame(
            ['Michael'],
            array_dot_get(
                ['users' => [['user' => ['id' => 1, 'name' => 'Michael']], ['user' => ['id' => 2]]]],
                'users.?*.user.name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_asterix_and_different_elements_using_nullsafe() : void
    {
        $this->assertSame(
            ['Michael', null],
            array_dot_get(
                ['users' => [['user' => ['id' => 1, 'name' => 'Michael']], ['user' => ['id' => 2]]]],
                'users.*.user.?name'
            ),
        );
    }

    public function test_accessing_array_scalar_value_by_path_with_escaped_nullable_asterix() : void
    {
        $this->assertSame(
            'Michael',
            array_dot_get(
                ['users' => ['?*' => ['id' => 1, 'name' => 'Michael']]],
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
                ['transactions' => [
                    ['id' => 1, 'packages' => [['label_id' => '12345'], ['label_id' => '22222']]],
                    ['id' => 1, 'packages' => [['label_id' => '3333']]],
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
                ['transactions' => [
                    ['id' => 1, 'packages' => [['label_id' => '12345'], ['label_id' => '22222']]],
                    ['id' => 1, 'packages' => [['label_id' => '3333']]],
                    ['id' => 1, 'packages' => [['foo' => 'bar']]],
                ],
                ],
                'transactions.*.packages.*.?label_id'
            ),
        );
    }
}
