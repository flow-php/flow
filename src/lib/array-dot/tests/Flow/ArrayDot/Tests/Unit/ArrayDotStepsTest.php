<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use Flow\ArrayDot\Exception\InvalidPathException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\ArrayDot\array_dot_steps;

final class ArrayDotStepsTest extends TestCase
{
    public function test_empty_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage("Path can't be empty");

        // @mago-ignore analysis:deprecated-function
        array_dot_steps('');
    }

    public function test_escaping_dots(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo.bar'], array_dot_steps('foo\\.bar'));
    }

    public function test_escaping_multimatch(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '\\{bas, bai\\}'], array_dot_steps('foo.bar.\\{bas, bai\\}'));
    }

    public function test_multimatch(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '{bas, bai}'], array_dot_steps('foo.bar.{bas, bai}'));
    }

    public function test_multimatch_not_closed(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch must be used at the end of path');

        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '{bas, bai}'], array_dot_steps('foo.bar.{bas, bai.id'));
    }

    public function test_multimatch_not_closed_at_the_end_of_path(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch must be used at the end of path');

        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '{bas, bai}'], array_dot_steps('foo.bar.{bas, bai}.id'));
    }

    public function test_simple_multimatch(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['{foo}'], array_dot_steps('{foo}'));
    }

    public function test_simple_steps(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', 'baz'], array_dot_steps('foo.bar.baz'));
    }

    public function test_simple_steps_with_nullsafe(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', '?bar', 'baz'], array_dot_steps('foo.?bar.baz'));
    }

    public function test_simple_steps_with_nullsafe_wildcard(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '?*', 'baz'], array_dot_steps('foo.bar.?*.baz'));
    }

    public function test_simple_steps_with_wildcard(): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame(['foo', 'bar', '*', 'baz'], array_dot_steps('foo.bar.*.baz'));
    }

    /**
     * @param list<string> $expected
     */
    #[TestWith(['a\\{b', ['a\\{b']])]
    #[TestWith(['a\\{b.c', ['a\\{b', 'c']])]
    #[TestWith(['x.\\{a,b\\}.c', ['x', '\\{a,b\\}', 'c']])]
    #[TestWith(['x.{a\\.b,c}', ['x', '{a\\.b,c}']])]
    #[TestWith(['{a\\.b}', ['{a\\.b}']])]
    public function test_escaped_braces_never_start_a_multimatch(string $path, array $expected): void
    {
        // @mago-ignore analysis:deprecated-function
        static::assertSame($expected, array_dot_steps($path));
    }
}
