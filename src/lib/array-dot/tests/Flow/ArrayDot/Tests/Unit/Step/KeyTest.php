<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit\Step;

use Flow\ArrayDot\Step\Key;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class KeyTest extends TestCase
{
    #[TestWith(['a', false, 'a'])]
    #[TestWith(['a', true, '?a'])]
    #[TestWith([0, false, '0'])]
    #[TestWith(['\\.?*,{}', true, '?\\\\\\.\\?\\*\\,\\{\\}'])]
    public function test_to_string_escapes_every_grammar_character(
        int|string $name,
        bool $nullsafe,
        string $expected,
    ): void {
        static::assertSame($expected, (new Key($name, $nullsafe))->toString());
    }
}
