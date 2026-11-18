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

    public function test_guide_card_front_matter(): void
    {
        $guide = new Page(
            'x',
            "---\nproblem: One INSERT per row takes minutes\nproof_label: 100,000 rows\nproof_before: 264s\nproof_after: 2.3s\nfeatured: true\n---\n# T\n",
        );

        static::assertSame(
            [
                'problem' => 'One INSERT per row takes minutes',
                'proof_label' => '100,000 rows',
                'proof_before' => '264s',
                'proof_after' => '2.3s',
                'featured' => true,
            ],
            $guide->card(),
        );
    }

    public function test_guide_card_front_matter_is_optional(): void
    {
        static::assertSame(
            [
                'problem' => null,
                'proof_label' => null,
                'proof_before' => null,
                'proof_after' => null,
                'featured' => false,
            ],
            (new Page('x', "---\nseo_title: T\n---\n# T\n"))->card(),
        );
    }

    public function test_missing_guide_front_matter_fails(): void
    {
        $this->expectException(InvalidTypeException::class);

        (new Page('x', "---\nseo_title: T\n---\n# T\n"))->group();
    }
}
