<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Component\DomCrawler\Crawler;

use function str_contains;

final class HomeControllerTest extends WebTestCase
{
    public function test_homepage_examples_link_to_topics(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/');

        self::assertResponseIsSuccessful();
        $topicLinks = $crawler->filter('nav a[href*="#example"]');
        static::assertGreaterThan(0, $topicLinks->count());
    }

    public function test_homepage_has_playground_link(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/');

        self::assertResponseIsSuccessful();
        $link = $crawler->filter('a[href="/playground"]');
        static::assertGreaterThan(0, $link->count());
    }

    public function test_homepage_returns_200(): void
    {
        $client = self::createClient();
        $client->request('GET', '/');

        self::assertResponseIsSuccessful();
    }

    public function test_homepage_topic_link_works(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/');

        $link = $crawler->filter('nav a[href*="#example"]')->first()->link();
        $client->click($link);

        self::assertResponseIsSuccessful();
    }

    public function test_not_found_page_links_to_docs_examples_and_search(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/404.html');

        self::assertResponseIsSuccessful();
        static::assertGreaterThan(0, $crawler->filter('main a[href="/documentation"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href^="/reading/array/"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main button[popovertarget="search-modal"]')->count());
        static::assertCount(1, $crawler
            ->filter('script')
            ->reduce(static fn(Crawler $script): bool => str_contains(
                $script->text(),
                "window.posthog.capture('site:not_found_view')",
            )));
        static::assertSame(1, $crawler->filter('meta[name="robots"][content="noindex"]')->count());
    }

    public function test_sponsor_page_sends_sponsors_to_the_work_shop(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/sponsor');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        // Sponsoring funds maintenance, it never buys or gates anything - the page must say so.
        static::assertStringContainsString('going to stay MIT', $text);
        static::assertStringContainsString('even the smallest help', $text);

        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop#sponsoring"]')->count());
        // Sponsoring moved from GitHub Sponsors to the Work-Shop.
        static::assertSame(0, $crawler->filter('main a[href^="https://github.com/sponsors"]')->count());

        // Buying a blueprint supports the project too, so the page promotes it.
        static::assertStringContainsString('Support by learning', $text);
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop#blueprints"]')->count());
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
