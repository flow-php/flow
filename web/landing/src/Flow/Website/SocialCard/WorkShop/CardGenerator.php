<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard\WorkShop;

use Flow\Website\SocialCard\CardPainter;
use Flow\Website\WorkShop\Listing;
use GdImage;
use RuntimeException;

use function dirname;
use function imagepng;
use function is_dir;
use function mkdir;
use function sprintf;

final readonly class CardGenerator
{
    public function __construct(
        private CardPainter $painter,
        private string $publicDir,
    ) {}

    public function generate(Listing $listing): string
    {
        $canvas = $this->painter->canvas();

        $this->painter->drawElephant($canvas);
        $this->painter->drawEyebrow($canvas, 'WORK-SHOP');
        $this->painter->drawDescription($canvas, $listing->blurb, $this->painter->drawTitle($canvas, $listing->name));

        if ($listing->price !== null) {
            $this->painter->drawFooter($canvas, $listing->price, $listing->category, primaryColor: CardPainter::ORANGE);
        } else {
            $this->painter->drawFooter($canvas, $listing->category, primaryColor: CardPainter::ORANGE);
        }

        $this->painter->drawAccentBar($canvas);

        return $this->save($canvas, $listing);
    }

    public function relativePath(Listing $listing): string
    {
        return sprintf('/images/work-shop/social/%s.png', $listing->slug);
    }

    public function save(GdImage $canvas, Listing $listing): string
    {
        $path = $this->publicDir . $this->relativePath($listing);
        $directory = dirname($path);

        if (!is_dir($directory) && !mkdir($directory, permissions: 0o755, recursive: true) && !is_dir($directory)) {
            throw new RuntimeException('Failed to create directory: ' . $directory);
        }

        if (imagepng($canvas, $path) === false) {
            throw new RuntimeException('Failed to write social card: ' . $path);
        }

        return $this->relativePath($listing);
    }
}
