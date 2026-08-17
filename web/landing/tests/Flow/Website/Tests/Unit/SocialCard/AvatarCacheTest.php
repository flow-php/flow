<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard;

use Flow\Website\SocialCard\AvatarCache;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

use function file_exists;
use function file_put_contents;
use function imagecreatetruecolor;
use function imagepng;
use function mkdir;
use function ob_get_clean;
use function ob_start;
use function sys_get_temp_dir;
use function uniqid;

final class AvatarCacheTest extends TestCase
{
    public function test_fetches_and_caches_an_avatar(): void
    {
        $cacheDir = sys_get_temp_dir() . '/' . uniqid('avatar-cache-', more_entropy: true);
        ob_start();
        imagepng(imagecreatetruecolor(width: 8, height: 8));
        $avatar = (string) ob_get_clean();

        $cache = new AvatarCache(new MockHttpClient(new MockResponse($avatar)), $cacheDir);

        static::assertSame($cacheDir . '/norberttech.png', $cache->fetch('norberttech'));
        static::assertStringEqualsFile($cacheDir . '/norberttech.png', $avatar);
    }

    public function test_returns_already_cached_avatar_without_fetching(): void
    {
        $cacheDir = sys_get_temp_dir() . '/' . uniqid('avatar-cache-', more_entropy: true);
        mkdir($cacheDir, permissions: 0o755, recursive: true);
        file_put_contents($cacheDir . '/norberttech.png', data: 'cached-bytes');

        $cache = new AvatarCache(new MockHttpClient([]), $cacheDir);

        static::assertSame($cacheDir . '/norberttech.png', $cache->fetch('norberttech'));
        static::assertStringEqualsFile($cacheDir . '/norberttech.png', 'cached-bytes');
    }

    public function test_returns_null_when_fetch_fails(): void
    {
        $cacheDir = sys_get_temp_dir() . '/' . uniqid('avatar-cache-', more_entropy: true);

        $cache = new AvatarCache(new MockHttpClient(new MockResponse('', ['http_code' => 404])), $cacheDir);

        static::assertNull($cache->fetch('norberttech'));
        static::assertFalse(file_exists($cacheDir . '/norberttech.png'));
    }

    public function test_returns_null_when_response_is_not_an_image(): void
    {
        $cacheDir = sys_get_temp_dir() . '/' . uniqid('avatar-cache-', more_entropy: true);

        $cache = new AvatarCache(new MockHttpClient(new MockResponse('<html>Not Found</html>')), $cacheDir);

        static::assertNull($cache->fetch('norberttech'));
        static::assertFalse(file_exists($cacheDir . '/norberttech.png'));
    }
}
