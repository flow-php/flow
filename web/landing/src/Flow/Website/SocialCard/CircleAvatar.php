<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard;

use GdImage;

use function file_exists;
use function file_get_contents;
use function getimagesizefromstring;
use function imagealphablending;
use function imagecolorallocatealpha;
use function imagecopy;
use function imagecopyresampled;
use function imagecreatefromstring;
use function imagecreatetruecolor;
use function imagesavealpha;
use function imagesetpixel;
use function imagesx;
use function imagesy;

final readonly class CircleAvatar
{
    public function draw(GdImage $canvas, string $avatarPath, int $x, int $y, int $size): bool
    {
        $avatar = $this->load($avatarPath);

        if ($avatar === null) {
            return false;
        }

        $circle = imagecreatetruecolor($size, $size);

        if ($circle === false) {
            return false;
        }

        imagecopyresampled(
            $circle,
            $avatar,
            dst_x: 0,
            dst_y: 0,
            src_x: 0,
            src_y: 0,
            dst_width: $size,
            dst_height: $size,
            src_width: imagesx($avatar),
            src_height: imagesy($avatar),
        );

        if (!$this->mask($circle, $size)) {
            return false;
        }

        imagecopy($canvas, $circle, dst_x: $x, dst_y: $y, src_x: 0, src_y: 0, src_width: $size, src_height: $size);

        return true;
    }

    public function load(string $avatarPath): ?GdImage
    {
        if (!file_exists($avatarPath)) {
            return null;
        }

        $bytes = file_get_contents($avatarPath);

        if ($bytes === false || getimagesizefromstring($bytes) === false) {
            return null;
        }

        $avatar = imagecreatefromstring($bytes);

        return $avatar === false ? null : $avatar;
    }

    public function mask(GdImage $circle, int $size): bool
    {
        imagealphablending($circle, enable: false);
        imagesavealpha($circle, enable: true);
        $transparent = imagecolorallocatealpha($circle, red: 0, green: 0, blue: 0, alpha: 127);

        if ($transparent === false) {
            return false;
        }

        $radius = $size / 2;

        for ($pixelX = 0; $pixelX < $size; $pixelX++) {
            for ($pixelY = 0; $pixelY < $size; $pixelY++) {
                $dx = $pixelX - $radius + 0.5;
                $dy = $pixelY - $radius + 0.5;

                if ((($dx * $dx) + ($dy * $dy)) > ($radius * $radius)) {
                    imagesetpixel($circle, $pixelX, $pixelY, $transparent);
                }
            }
        }

        return true;
    }
}
