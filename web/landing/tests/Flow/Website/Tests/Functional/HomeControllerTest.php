<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Component\DomCrawler\Crawler;
use Twig\Environment;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function preg_replace;
use function str_contains;

final class HomeControllerTest extends WebTestCase
{
    public function test_homepage_analytics_and_search_markup(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/');

        self::assertResponseIsSuccessful();

        foreach ([
            'head script[src="/posthog.js"]',
            'head link[href="/pagefind/pagefind-component-ui.css"]',
            '#site-search pagefind-config[bundle-path="/pagefind/"]',
            '#site-search pagefind-input',
            '#site-search pagefind-filter-pane',
            '#site-search pagefind-summary',
            '#site-search pagefind-results',
            'header[data-ph-capture-attribute-link-location="header"]',
            'nav#mobile-nav[data-ph-capture-attribute-link-location="header"]',
            'footer[data-ph-capture-attribute-link-location="footer"]',
            'section[data-ph-capture-attribute-link-location="hero"]',
            'section a[href="/playground"][data-ph-capture-attribute-cta="hero_playground"]',
            'section a[href="/documentation"][data-ph-capture-attribute-cta="hero_docs"]',
            'a[href="/playground"][data-ph-capture-attribute-cta="home_playground"]',
            'a[href="/documentation"][data-ph-capture-attribute-cta="home_docs"]',
            '[data-ph-capture-attribute-link-location="hero"] a[href="https://packagist.org/packages/flow-php/etl"][target="_blank"][rel="noopener"]',
            'footer li a[href="https://packagist.org/packages/flow-php/etl"][target="_blank"][rel="noopener"]',
        ] as $element) {
            static::assertCount(1, $crawler->filter($element), $element);
        }

        static::assertCount(0, $crawler->filter('link[href="/pagefind/pagefind-ui.css"]'));
    }

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
                "window.posthog?.capture('site:not_found_view')",
            )));
        static::assertSame(1, $crawler->filter('meta[name="robots"][content="noindex"]')->count());
    }

    public function test_posthog_loader(): void
    {
        $client = self::createClient();
        $client->request('GET', '/posthog.js');

        self::assertResponseIsSuccessful();
        self::assertResponseHeaderSame('Content-Type', 'text/javascript; charset=UTF-8');
        static::assertSame('', $client->getResponse()->getContent());

        $loader = type_instance_of(Environment::class)
            ->assert(self::getContainer()->get(Environment::class))
            ->render('main/posthog.js.twig', [
                'posthog_key' => 'phc_test',
                'posthog_api_host' => 'api.test',
                'posthog_ui_host' => 'ui.test',
            ]);

        static::assertStringContainsString('/static/array.js', $loader);
        static::assertStringContainsString(
            "posthog.init('phc_test', { api_host: 'api.test', ui_host: 'ui.test', defaults: '2026-01-30', cookieless_mode: 'always' })",
            type_string()->assert(preg_replace('/\s+/', ' ', $loader)),
        );
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
