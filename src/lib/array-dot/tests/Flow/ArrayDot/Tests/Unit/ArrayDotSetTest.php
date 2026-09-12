<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Tests\Unit\Fixtures\UnknownStep;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ArrayDot\array_dot_set;

final class ArrayDotSetTest extends TestCase
{
    public function test_replace_value_on_non_empty_array(): void
    {
        static::assertSame(
            [
                'foo' => [
                    'bar' => 'baz',
                ],
                'fos' => 1,
            ],
            array_dot_set(
                [
                    'foo' => [
                        'bar' => 'caz',
                    ],
                    'fos' => 1,
                ],
                'foo.bar',
                'baz',
            ),
        );
    }

    public function test_set_value_on_empty_array(): void
    {
        static::assertSame(
            [
                'foo' => [
                    'bar' => 'baz',
                ],
            ],
            array_dot_set([], 'foo.bar', 'baz'),
        );
    }

    public function test_set_value_on_empty_array_using_escaped_wildcard(): void
    {
        static::assertSame(
            [
                'foo' => [
                    '*' => 'baz',
                ],
            ],
            array_dot_set([], 'foo.\\*', 'baz'),
        );
    }

    public function test_set_value_on_existing_nested_array_each_element(): void
    {
        static::assertSame(
            [
                'users' => [
                    [
                        'id' => 1,
                        'status' => 'active',
                    ],
                    [
                        'id' => 2,
                        'status' => 'active',
                    ],
                ],
            ],
            array_dot_set(
                [
                    'users' => [
                        [
                            'id' => 1,
                        ],
                        [
                            'id' => 2,
                        ],
                    ],
                ],
                'users.*.status',
                'active',
            ),
        );
    }

    public function test_set_keeps_siblings_at_every_depth(): void
    {
        static::assertSame(
            ['a' => ['x' => 1, 'y' => 3], 'b' => 2],
            array_dot_set(['a' => ['x' => 1], 'b' => 2], 'a.y', 3),
        );
    }

    public function test_set_keeps_integer_keys(): void
    {
        static::assertSame([5 => 'c', 7 => 'b'], array_dot_set([5 => 'a', 7 => 'b'], '5', 'c'));
    }

    /**
     * @param array<mixed> $expected
     */
    #[TestWith(['\\{a\\}', ['{a}' => 1]])]
    #[TestWith(['?a', ['a' => 1]])]
    #[TestWith(['a\\.b', ['a.b' => 1]])]
    public function test_set_resolves_escapes_and_the_nullsafe_marker(string $path, array $expected): void
    {
        static::assertSame($expected, array_dot_set([], $path, 1));
    }

    public function test_set_escaped_dot_after_a_wildcard(): void
    {
        static::assertSame(['x' => [['a.b' => 1], ['a.b' => 1]]], array_dot_set(['x' => [[], []]], 'x.*.a\\.b', 1));
    }

    public function test_set_trailing_wildcard_sets_every_element(): void
    {
        static::assertSame(['x' => [0, 0]], array_dot_set(['x' => [1, 2]], 'x.*', 0));
    }

    public function test_set_refuses_a_multimatch(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Path "x.{a,b}" ends with a multimatch');

        array_dot_set([], 'x.{a,b}', 1);
    }

    public function test_set_through_a_path_object(): void
    {
        static::assertSame(['a.b' => 1], array_dot_set([], new Path([new Key('a.b')]), 1));
    }

    /**
     * @param array<mixed> $array
     */
    #[TestWith([[], 'x.*'])]
    #[TestWith([['x' => 5], 'x.*'])]
    #[TestWith([['x' => null], 'x.*'])]
    #[TestWith([[], 'x.*.a'])]
    public function test_set_through_a_wildcard_over_a_missing_or_non_array_key_leaves_it_empty(
        array $array,
        string $path,
    ): void {
        static::assertSame(['x' => []], array_dot_set($array, $path, 1));
    }

    public function test_set_fails_for_an_unknown_step(): void
    {
        $this->expectException(InvalidTypeException::class);

        array_dot_set(['a' => 1], new Path([new UnknownStep()]), 1);
    }
}
