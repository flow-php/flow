<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Model\Documentation;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Website\Model\Documentation\Page;
use PHPUnit\Framework\TestCase;

final class PageTest extends TestCase
{
    public function test_guide_front_matter(): void
    {
        $guide = new Page(
            'x',
            "---\nseo_title: T\ngroup: Reading files\npublished: '2026-11-18'\nupdated: '2026-11-19'\n---\n# T\n",
        );

        static::assertSame('Reading files', $guide->group());
        static::assertSame('2026-11-18', $guide->published());
        static::assertSame('2026-11-19', $guide->updated());
    }

    public function test_missing_guide_front_matter_fails(): void
    {
        $this->expectException(InvalidTypeException::class);

        (new Page('x', "---\nseo_title: T\n---\n# T\n"))->group();
    }
}
