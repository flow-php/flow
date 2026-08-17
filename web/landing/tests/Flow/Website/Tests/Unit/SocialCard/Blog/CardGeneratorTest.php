<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\SocialCard\Blog;

use Flow\Website\Blog\Post;
use Flow\Website\SocialCard\AvatarCache;
use Flow\Website\SocialCard\Blog\CardGenerator;
use Flow\Website\SocialCard\CardPainter;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

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
        $generator = new CardGenerator(
            new CardPainter(dirname(__DIR__, levels: 7) . '/resources'),
            new AvatarCache(new MockHttpClient(new MockResponse('', ['http_code' => 404])), $publicDir . '/avatars'),
            $publicDir,
        );
        $post = Post::fromArray([
            'title' => 'Consuming APIs Without SDKs',
            'description' => 'How to consume any HTTP API with the Flow PHP DataFrame API, without vendor SDKs.',
            'date' => '2026-08-14',
            'slug' => 'consuming-apis-without-sdks',
            'author' => 'norberttech',
        ]);

        $path = $generator->generate($post);

        static::assertSame('/images/blog/social/2026-08-14-consuming-apis-without-sdks.png', $path);
        static::assertFileExists($publicDir . $path);
        static::assertSame(
            [1200, 630, IMAGETYPE_PNG],
            array_slice((array) getimagesize($publicDir . $path), offset: 0, length: 3),
        );

        unlink($publicDir . $path);
    }

    public function test_generates_a_card_for_a_long_title_and_description(): void
    {
        $publicDir = sys_get_temp_dir() . '/' . uniqid('social-card-', more_entropy: true);
        $generator = new CardGenerator(
            new CardPainter(dirname(__DIR__, levels: 7) . '/resources'),
            new AvatarCache(new MockHttpClient(new MockResponse('', ['http_code' => 404])), $publicDir . '/avatars'),
            $publicDir,
        );
        $post = Post::fromArray([
            'title' => 'A Very Long Blog Post Title That Definitely Does Not Fit Into A Single Line Of The Social Card And Must Be Wrapped And Ellipsized',
            'description' => 'An equally long description that goes on and on about data processing, extractors, transformers, loaders, schema inference, type systems and everything else that makes Flow PHP a unified data processing framework.',
            'date' => '2026-08-14',
            'slug' => 'long-title',
            'author' => 'norberttech',
        ]);

        $path = $generator->generate($post);

        static::assertFileExists($publicDir . $path);
        static::assertSame(
            [1200, 630, IMAGETYPE_PNG],
            array_slice((array) getimagesize($publicDir . $path), offset: 0, length: 3),
        );

        unlink($publicDir . $path);
    }
}
