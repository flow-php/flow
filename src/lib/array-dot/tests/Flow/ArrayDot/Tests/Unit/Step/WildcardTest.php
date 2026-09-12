<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit\Step;

use Flow\ArrayDot\Step\Wildcard;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class WildcardTest extends TestCase
{
    #[TestWith([false, '*'])]
    #[TestWith([true, '?*'])]
    public function test_to_string(bool $nullsafe, string $expected): void
    {
        static::assertSame($expected, (new Wildcard($nullsafe))->toString());
    }
}
