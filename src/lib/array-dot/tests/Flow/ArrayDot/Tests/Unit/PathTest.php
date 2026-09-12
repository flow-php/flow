<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class PathTest extends TestCase
{
    /**
     * @return array<string, array{string, Path}>
     */
    public static function parsed_paths(): array
    {
        return [
            'keys' => ['foo.bar', new Path([new Key('foo'), new Key('bar')])],
            'nullsafe key' => ['foo.?bar', new Path([new Key('foo'), new Key('bar', nullsafe: true)])],
            'repeated nullsafe marker' => ['??x', new Path([new Key('x', nullsafe: true)])],
            'wildcard' => ['foo.*.bar', new Path([new Key('foo'), new Wildcard(), new Key('bar')])],
            'nullsafe wildcard' => [
                'foo.?*.bar',
                new Path([new Key('foo'), new Wildcard(nullsafe: true), new Key('bar')]),
            ],
            'escaped wildcard' => ['\\*', new Path([new Key('*')])],
            'escaped nullsafe wildcard' => ['\\?*', new Path([new Key('?*')])],
            'escaped dot' => ['a\\.b', new Path([new Key('a.b')])],
            'escaped braces' => ['\\{a\\}', new Path([new Key('{a}')])],
            'brace inside a key' => ['a{b', new Path([new Key('a{b')])],
            'braces inside a key' => ['a{b}', new Path([new Key('a{b}')])],
            'star inside a key' => ['a*b', new Path([new Key('a*b')])],
            'question mark inside a key' => ['a?b', new Path([new Key('a?b')])],
            'escaped leading question mark' => ['\\?x', new Path([new Key('?x')])],
            'escaped backslash' => ['a\\\\', new Path([new Key('a\\')])],
            'backslash before a plain character' => ['a\\b', new Path([new Key('a\\b')])],
            'escaped backslash before a separator' => ['a\\\\.b', new Path([new Key('a\\'), new Key('b')])],
            'empty key' => ['a..b', new Path([new Key('a'), new Key(''), new Key('b')])],
            'numeric keys' => ['0.1', new Path([new Key('0'), new Key('1')])],
            'root multimatch' => [
                '{a,b}',
                new Path([new Multimatch([new Path([new Key('a')]), new Path([new Key('b')])])]),
            ],
            'multimatch of trimmed paths' => [
                'x.{bas, ?bai.id}',
                new Path([
                    new Key('x'),
                    new Multimatch([
                        new Path([new Key('bas')]),
                        new Path([new Key('bai', nullsafe: true), new Key('id')]),
                    ]),
                ]),
            ],
            'escaped comma in a multimatch' => [
                'x.{a\\,b,c}',
                new Path([new Key('x'), new Multimatch([new Path([new Key('a,b')]), new Path([new Key('c')])])]),
            ],
            'escaped braces before another step' => [
                'x.\\{a,b\\}.c',
                new Path([new Key('x'), new Key('{a,b}'), new Key('c')]),
            ],
            'trailing empty key' => ['a.', new Path([new Key('a'), new Key('')])],
            'trailing nullsafe empty key' => ['a.?', new Path([new Key('a'), new Key('', nullsafe: true)])],
            'trailing lone backslash' => ['a\\', new Path([new Key('a\\')])],
            'escaped dot in a multimatch' => [
                'x.{a\\.b}',
                new Path([new Key('x'), new Multimatch([new Path([new Key('a.b')])])]),
            ],
        ];
    }

    #[DataProvider('parsed_paths')]
    public function test_parses_a_string_into_steps(string $path, Path $expected): void
    {
        static::assertEquals($expected, Path::fromString($path));
    }

    #[DataProvider('parsed_paths')]
    public function test_to_string_round_trips(string $path, Path $expected): void
    {
        static::assertEquals($expected, Path::fromString($expected->toString()));
    }

    public function test_to_string_joins_the_steps(): void
    {
        static::assertSame('a.\\*.*.?*.{b,?c.d}', Path::fromString('a.\\*.*.?*.{b, ?c.d}')->toString());
    }

    #[TestWith(['a.?b', true])]
    #[TestWith(['\\*', true])]
    #[TestWith(['a.*', false])]
    #[TestWith(['a.?*', false])]
    #[TestWith(['{a,b}', false])]
    public function test_selects_a_single_value_only_through_keys(string $path, bool $single): void
    {
        static::assertSame($single, Path::fromString($path)->selectsSingleValue());
    }

    #[TestWith(['', "Path can't be empty."])]
    #[TestWith(['x.{a', 'Multimatch must be used at the end of path'])]
    #[TestWith(['x.{a}.b', 'Multimatch must be used at the end of path'])]
    #[TestWith(['x.{a,}', "Path can't be empty."])]
    #[TestWith(['?{a}', 'Nullsafe "?" cannot precede a multimatch'])]
    public function test_rejects_an_invalid_string(string $path, string $message): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage($message);

        Path::fromString($path);
    }

    public function test_rejects_no_steps(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage("Path can't be empty.");

        new Path([]);
    }

    public function test_reindexes_the_steps(): void
    {
        static::assertEquals(new Path([new Key('a'), new Key('b')]), new Path([5 => new Key('a'), 9 => new Key('b')]));
    }

    public function test_rejects_a_multimatch_before_the_last_step(): void
    {
        $this->expectException(InvalidPathException::class);
        $this->expectExceptionMessage('Multimatch must be used at the end of path');

        new Path([new Multimatch([new Path([new Key('a')])]), new Key('b')]);
    }
}
