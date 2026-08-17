<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard;

use Flow\Website\SocialCard\CardPainter;
use PHPUnit\Framework\TestCase;

use function dirname;
use function imagecolorat;
use function imagesx;
use function imagesy;

final class CardPainterTest extends TestCase
{
    public function test_canvas_has_open_graph_dimensions_and_brand_background_gradient(): void
    {
        $canvas = (new CardPainter(dirname(__DIR__, levels: 6) . '/resources'))->canvas();

        static::assertSame(CardPainter::WIDTH, imagesx($canvas));
        static::assertSame(CardPainter::HEIGHT, imagesy($canvas));
        static::assertSame(0x26_2238, imagecolorat($canvas, x: 10, y: 0));
        static::assertNotSame(
            imagecolorat($canvas, x: 10, y: 0),
            imagecolorat($canvas, x: 10, y: CardPainter::HEIGHT - 1),
        );
    }

    public function test_draw_title_moves_the_baseline_further_down_for_longer_titles(): void
    {
        $painter = new CardPainter(dirname(__DIR__, levels: 6) . '/resources');

        $shortTitleBaseline = $painter->drawTitle($painter->canvas(), 'Short');
        $longTitleBaseline = $painter->drawTitle(
            $painter->canvas(),
            'A Considerably Longer Blog Post Title That Wraps Into Multiple Lines',
        );

        static::assertGreaterThan($shortTitleBaseline, $longTitleBaseline);
    }

    public function test_drawing_changes_canvas_pixels(): void
    {
        $painter = new CardPainter(dirname(__DIR__, levels: 6) . '/resources');
        $canvas = $painter->canvas();

        $before = imagecolorat($canvas, x: 0, y: CardPainter::HEIGHT - 2);
        $painter->drawAccentBar($canvas);

        static::assertNotSame($before, imagecolorat($canvas, x: 0, y: CardPainter::HEIGHT - 2));
    }
}
