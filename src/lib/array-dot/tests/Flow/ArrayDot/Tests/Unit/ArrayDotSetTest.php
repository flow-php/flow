<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use function Flow\ArrayDot\array_dot_set;
use PHPUnit\Framework\TestCase;

final class ArrayDotSetTest extends TestCase
{
    public function test_replace_value_on_non_empty_array() : void
    {
        $this->assertSame(
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
                'baz'
            )
        );
    }

    public function test_set_value_on_empty_array() : void
    {
        $this->assertSame(
            [
                'foo' => [
                    'bar' => 'baz',
                ],
            ],
            array_dot_set([], 'foo.bar', 'baz')
        );
    }

    public function test_set_value_on_empty_array_using_escaped_wildcard() : void
    {
        $this->assertSame(
            [
                'foo' => [
                    '*' => 'baz',
                ],
            ],
            array_dot_set([], 'foo.\\*', 'baz')
        );
    }

    public function test_set_value_on_existing_nested_array_each_element() : void
    {
        $this->assertSame(
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
                'active'
            )
        );
    }
}
