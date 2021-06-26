<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use function Flow\ArrayDot\array_dot_steps;
use Flow\ArrayDot\Exception\InvalidPathException;
use PHPUnit\Framework\TestCase;

final class ArrayDotStepsTest extends TestCase
{
    public function test_empty_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage("Path can't be empty");

        array_dot_steps('');
    }

    public function test_simple_steps() : void
    {
        $this->assertSame(
            ['foo', 'bar', 'baz'],
            array_dot_steps('foo.bar.baz')
        );
    }

    public function test_simple_steps_with_nullsafe() : void
    {
        $this->assertSame(
            ['foo', '?bar', 'baz'],
            array_dot_steps('foo.?bar.baz')
        );
    }

    public function test_simple_steps_with_wildcard() : void
    {
        $this->assertSame(
            ['foo', 'bar', '*', 'baz'],
            array_dot_steps('foo.bar.*.baz')
        );
    }

    public function test_simple_steps_with_nullsafe_wildcard() : void
    {
        $this->assertSame(
            ['foo', 'bar', '?*', 'baz'],
            array_dot_steps('foo.bar.?*.baz')
        );
    }

    public function test_escaping_dots() : void
    {
        $this->assertSame(
            ['foo.bar'],
            array_dot_steps('foo\\.bar')
        );
    }

    public function test_multimatch() : void
    {
        $this->assertSame(
            ['foo', 'bar', '{bas, bai}'],
            array_dot_steps('foo.bar.{bas, bai}')
        );
    }

    public function test_escaping_multimatch() : void
    {
        $this->assertSame(
            ['foo', 'bar', '\\{bas, bai\\}'],
            array_dot_steps('foo.bar.\\{bas, bai\\}')
        );
    }

    public function test_multimatch_not_closed_at_the_end_of_path() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch must be used at the end of path');

        $this->assertSame(
            ['foo', 'bar', '{bas, bai}'],
            array_dot_steps('foo.bar.{bas, bai}.id')
        );
    }

    public function test_multimatch_not_closed() : void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch syntax not closed');

        $this->assertSame(
            ['foo', 'bar', '{bas, bai}'],
            array_dot_steps('foo.bar.{bas, bai.id')
        );
    }
}
