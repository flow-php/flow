<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard;

use Flow\Website\SocialCard\CircleAvatar;
use PHPUnit\Framework\TestCase;

use function file_put_contents;
use function imagecolorallocate;
use function imagecolorat;
use function imagecreatetruecolor;
use function imagefill;
use function imagepng;
use function mkdir;
use function sys_get_temp_dir;
use function uniqid;

final class CircleAvatarTest extends TestCase
{
    public function test_draws_avatar_pixels_inside_the_circle(): void
    {
        $workDir = sys_get_temp_dir() . '/' . uniqid('circle-avatar-', more_entropy: true);
        mkdir($workDir, permissions: 0o755, recursive: true);

        $avatar = imagecreatetruecolor(width: 8, height: 8);
        static::assertNotFalse($avatar);
        imagefill($avatar, x: 0, y: 0, color: (int) imagecolorallocate($avatar, red: 255, green: 0, blue: 0));
        imagepng($avatar, $workDir . '/avatar.png');

        $canvas = imagecreatetruecolor(width: 100, height: 100);
        static::assertNotFalse($canvas);

        static::assertTrue((new CircleAvatar())->draw($canvas, $workDir . '/avatar.png', x: 10, y: 10, size: 48));
        static::assertSame(0xFF_0000, imagecolorat($canvas, x: 34, y: 34));
    }

    public function test_returns_false_for_missing_avatar_file(): void
    {
        $canvas = imagecreatetruecolor(width: 100, height: 100);
        static::assertNotFalse($canvas);

        static::assertFalse((new CircleAvatar())->draw($canvas, '/nonexistent/avatar.png', x: 0, y: 0, size: 48));
    }

    public function test_returns_false_for_a_file_that_is_not_an_image(): void
    {
        $workDir = sys_get_temp_dir() . '/' . uniqid('circle-avatar-', more_entropy: true);
        mkdir($workDir, permissions: 0o755, recursive: true);
        file_put_contents($workDir . '/avatar.png', data: 'not-an-image');

        $canvas = imagecreatetruecolor(width: 100, height: 100);
        static::assertNotFalse($canvas);

        static::assertFalse((new CircleAvatar())->draw($canvas, $workDir . '/avatar.png', x: 0, y: 0, size: 48));
    }
}
