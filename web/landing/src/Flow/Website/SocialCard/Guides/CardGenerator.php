<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard\Guides;

use Flow\Website\Model\Documentation\Page;
use Flow\Website\SocialCard\CardPainter;
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

    public function generate(Page $guide, string $slug): string
    {
        $canvas = $this->painter->canvas();
        $card = $guide->card();

        $this->painter->drawElephant($canvas);
        $this->painter->drawEyebrow($canvas, section: 'GUIDE');
        $this->painter->drawDescription(
            $canvas,
            (string) $card['problem'],
            $this->painter->drawTitle($canvas, (string) $guide->title()),
        );

        if ($card['proof_after'] !== null) {
            $this->painter->drawFooter(
                $canvas,
                $card['proof_before'] === null
                    ? $card['proof_after']
                    : $card['proof_before'] . ' to ' . $card['proof_after'],
                $card['proof_label'],
                primaryColor: CardPainter::ORANGE,
            );
        }

        $this->painter->drawAccentBar($canvas);

        $path = $this->publicDir . $this->relativePath($slug);

        if (
            !is_dir(dirname($path))
            && !mkdir(dirname($path), permissions: 0o755, recursive: true)
            && !is_dir(dirname($path))
        ) {
            throw new RuntimeException('Failed to create directory: ' . dirname($path));
        }

        if (imagepng($canvas, $path) === false) {
            throw new RuntimeException('Failed to write social card: ' . $path);
        }

        return $this->relativePath($slug);
    }

    public function relativePath(string $slug): string
    {
        return sprintf('/images/guides/social/%s.png', $slug);
    }
}
