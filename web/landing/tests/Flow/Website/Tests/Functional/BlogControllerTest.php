<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Blog\Posts;
use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

use function dirname;
use function sprintf;

final class BlogControllerTest extends WebTestCase
{
    public function test_blog_index_lists_the_latest_post_first(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/blog');

        self::assertResponseIsSuccessful();
        static::assertStringContainsString(
            (new Posts())->all()[0]->title,
            $crawler->filter('article h2')->first()->text(),
        );
    }

    public function test_blog_post_page_generates_social_card_and_points_open_graph_image_to_it(): void
    {
        $post = (new Posts())->all()[0];
        $socialCardPath = sprintf('/images/blog/social/%s-%s.png', $post->date->format('Y-m-d'), $post->slug);

        $client = self::createClient();
        $crawler = $client->request('GET', sprintf('/blog/%s/%s', $post->date->format('Y-m-d'), $post->slug));

        self::assertResponseIsSuccessful();
        static::assertStringEndsWith(
            $socialCardPath,
            (string) $crawler->filter('meta[property="og:image"]')->attr('content'),
        );
        static::assertStringEndsWith(
            $socialCardPath,
            (string) $crawler->filter('meta[name="twitter:image"]')->attr('content'),
        );
        static::assertSame('article', $crawler->filter('meta[property="og:type"]')->attr('content'));
        static::assertFileExists(dirname(__DIR__, levels: 5) . '/public' . $socialCardPath);
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
