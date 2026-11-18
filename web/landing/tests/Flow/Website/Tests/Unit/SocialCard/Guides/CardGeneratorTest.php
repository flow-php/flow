<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard\Guides;

use Flow\Website\Model\Documentation\Page;
use Flow\Website\SocialCard\CardPainter;
use Flow\Website\SocialCard\Guides\CardGenerator;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_slice;
use function dirname;
use function getimagesize;
use function sys_get_temp_dir;
use function uniqid;
use function unlink;

final class CardGeneratorTest extends TestCase
{
    #[TestWith(["problem: Slow import\nproof_label: 100,000 rows\nproof_before: 264s\nproof_after: 2.3s\n"])]
    #[TestWith(["problem: LIKE is not a search\nproof_label: 100,000 products\nproof_after: 4s\n"])]
    #[TestWith(["problem: Nobody knows what the API sent\n"])]
    public function test_generates_a_png_card_with_open_graph_dimensions(string $card): void
    {
        $publicDir = sys_get_temp_dir() . '/' . uniqid('social-card-', more_entropy: true);

        $path = (new CardGenerator(new CardPainter(dirname(__DIR__, levels: 7) . '/resources'), $publicDir))->generate(
            new Page('import-csv', "---\nseo_title: Import a CSV file\n" . $card . "---\n# T\n"),
            slug: 'import-csv',
        );

        static::assertSame('/images/guides/social/import-csv.png', $path);
        static::assertSame(
            [1200, 630, IMAGETYPE_PNG],
            array_slice((array) getimagesize($publicDir . $path), offset: 0, length: 3),
        );

        unlink($publicDir . $path);
    }
}
