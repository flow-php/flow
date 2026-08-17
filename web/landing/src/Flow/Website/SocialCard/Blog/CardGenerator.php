<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard\Blog;

use Flow\Website\Blog\Post;
use Flow\Website\SocialCard\AvatarCache;
use Flow\Website\SocialCard\CardPainter;
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
        private AvatarCache $avatars,
        private string $publicDir,
    ) {}

    public function generate(Post $post): string
    {
        $canvas = $this->painter->canvas();

        $this->painter->drawElephant($canvas);
        $this->painter->drawEyebrow($canvas, 'BLOG');
        $this->painter->drawDescription($canvas, $post->description, $this->painter->drawTitle($canvas, $post->title));
        $this->painter->drawFooter(
            $canvas,
            '@' . $post->author,
            $post->date->format('M j, Y'),
            $this->avatars->fetch($post->author),
        );
        $this->painter->drawAccentBar($canvas);

        return $this->save($canvas, $post);
    }

    public function relativePath(Post $post): string
    {
        return sprintf('/images/blog/social/%s-%s.png', $post->date->format('Y-m-d'), $post->slug);
    }

    public function save(GdImage $canvas, Post $post): string
    {
        $path = $this->publicDir . $this->relativePath($post);
        $directory = dirname($path);

        if (!is_dir($directory) && !mkdir($directory, permissions: 0o755, recursive: true) && !is_dir($directory)) {
            throw new RuntimeException('Failed to create directory: ' . $directory);
        }

        if (imagepng($canvas, $path) === false) {
            throw new RuntimeException('Failed to write social card: ' . $path);
        }

        return $this->relativePath($post);
    }
}
