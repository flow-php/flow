<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit;

use Flow\PostgreSql\DeparseOptions;
use PHPUnit\Framework\TestCase;

final class DeparseOptionsTest extends TestCase
{
    public function test_preserve_comments_can_be_turned_on(): void
    {
        static::assertTrue(DeparseOptions::new()->preserveComments()->hasPreserveComments());
    }

    public function test_preserve_comments_false_turns_it_off(): void
    {
        static::assertFalse(DeparseOptions::new()->preserveComments()->preserveComments(false)->hasPreserveComments());
    }

    public function test_preserve_comments_is_off_by_default(): void
    {
        static::assertFalse(DeparseOptions::new()->hasPreserveComments());
    }
}
