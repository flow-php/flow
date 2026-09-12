<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit\Step;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;
use PHPUnit\Framework\TestCase;

use function array_values;

final class MultimatchTest extends TestCase
{
    public function test_keys_each_path_by_its_steps_joined_with_underscores(): void
    {
        $expected = [
            'id' => new Path([new Key('id')]),
            'a_b' => new Path([new Key('a', nullsafe: true), new Key('b')]),
            'list_*' => new Path([new Key('list'), new Wildcard()]),
            'a.b' => new Path([new Key('a.b')]),
        ];

        static::assertSame($expected, (new Multimatch(array_values($expected)))->byResultKey());
    }

    public function test_to_string_joins_its_paths(): void
    {
        static::assertSame(
            '{a,?b.c}',
            (new Multimatch([
                new Path([new Key('a')]),
                new Path([new Key('b', nullsafe: true), new Key('c')]),
            ]))->toString(),
        );
    }

    public function test_rejects_no_paths(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch needs at least one path.');

        new Multimatch([]);
    }

    public function test_rejects_a_nested_multimatch(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch cannot contain another multimatch.');

        new Multimatch([new Path([new Key('a'), new Multimatch([new Path([new Key('b')])])])]);
    }
}
