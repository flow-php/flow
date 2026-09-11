<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\FenceInfo;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class FenceInfoTest extends TestCase
{
    #[TestWith(['php', true])]
    #[TestWith(['php ', true])]
    #[TestWith(['  php  ', true])]
    #[TestWith(['php ignore', true])]
    #[TestWith(['text', false])]
    #[TestWith(['', false])]
    public function test_the_first_word_decides_the_language(string $info, bool $isPhp): void
    {
        static::assertSame($isPhp, FenceInfo::parse($info)->isPhp());
    }

    #[TestWith(['php ignore', true])]
    #[TestWith(['php', false])]
    #[TestWith(['php something', false])]
    #[TestWith(['php ignore extra', true])]
    public function test_only_the_ignore_modifier_marks_a_fence_as_deliberate(string $info, bool $ignored): void
    {
        static::assertSame($ignored, FenceInfo::parse($info)->isIgnored());
    }

    public function test_a_trailing_space_does_not_hide_a_php_fence(): void
    {
        static::assertSame('php', FenceInfo::parse('php ')->language);
        static::assertNull(FenceInfo::parse('php ')->modifier);
    }

    public function test_the_modifier_is_everything_after_the_language(): void
    {
        static::assertSame('ignore', FenceInfo::parse('php ignore')->modifier);
    }
}
