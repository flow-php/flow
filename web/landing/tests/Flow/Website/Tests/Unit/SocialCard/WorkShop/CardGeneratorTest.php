<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard\WorkShop;

use Flow\Website\SocialCard\CardPainter;
use Flow\Website\SocialCard\WorkShop\CardGenerator;
use Flow\Website\Tests\Mother\ListingMother;
use PHPUnit\Framework\TestCase;

use function array_slice;
use function dirname;
use function getimagesize;
use function sys_get_temp_dir;
use function uniqid;
use function unlink;

final class CardGeneratorTest extends TestCase
{
    public function test_generates_a_png_card_with_open_graph_dimensions(): void
    {
        $publicDir = sys_get_temp_dir() . '/' . uniqid('social-card-', more_entropy: true);
        $generator = new CardGenerator(new CardPainter(dirname(__DIR__, levels: 7) . '/resources'), $publicDir);

        $path = $generator->generate(ListingMother::priced());

        static::assertSame('/images/work-shop/social/symfony-backoffice.png', $path);
        static::assertFileExists($publicDir . $path);
        static::assertSame(
            [1200, 630, IMAGETYPE_PNG],
            array_slice((array) getimagesize($publicDir . $path), offset: 0, length: 3),
        );

        unlink($publicDir . $path);
    }

    public function test_generates_a_card_for_a_listing_without_a_price(): void
    {
        $publicDir = sys_get_temp_dir() . '/' . uniqid('social-card-', more_entropy: true);
        $generator = new CardGenerator(new CardPainter(dirname(__DIR__, levels: 7) . '/resources'), $publicDir);

        $path = $generator->generate(ListingMother::unpriced());

        static::assertSame('/images/work-shop/social/consulting.png', $path);
        static::assertFileExists($publicDir . $path);
        static::assertSame(
            [1200, 630, IMAGETYPE_PNG],
            array_slice((array) getimagesize($publicDir . $path), offset: 0, length: 3),
        );

        unlink($publicDir . $path);
    }
}
