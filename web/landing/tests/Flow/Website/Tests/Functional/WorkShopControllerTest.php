<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Component\Routing\RouterInterface;

use function Flow\Types\DSL\type_instance_of;

final class WorkShopControllerTest extends WebTestCase
{
    public function test_consulting_books_a_call_and_shows_photos(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/consulting');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('Book a free call', $text);
        // Consulting is quoted per engagement, so the page must explain the missing price.
        static::assertStringContainsString('Why there is no price here', $text);
        static::assertGreaterThan(0, $crawler->filter('main a[href^="https://calendar.proton.me/"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href^="mailto:"]')->count());
        static::assertSame(1, $crawler->filter('[data-controller~="work-shop-carousel"]')->count());
        static::assertGreaterThan(1, $crawler->filter('[data-work-shop-carousel-target="slide"]')->count());
    }

    public function test_legal_links_appear_only_where_something_is_sold(): void
    {
        $client = self::createClient();

        $blueprint = $client->request('GET', '/work-shop/blueprints/symfony-backoffice');
        self::assertResponseIsSuccessful();
        static::assertSame(1, $blueprint->filter('main a[href="/work-shop/terms-of-sales"]')->count());
        static::assertSame(1, $blueprint->filter('main a[href="/work-shop/privacy-policy"]')->count());

        // Consultations are arranged separately and are not governed by the Terms of Sale.
        $consulting = $client->request('GET', '/work-shop/consulting');
        self::assertResponseIsSuccessful();
        static::assertSame(0, $consulting->filter('main a[href="/work-shop/terms-of-sales"]')->count());
    }

    public function test_legal_pages_are_scoped_to_the_work_shop(): void
    {
        $client = self::createClient();

        $crawler = $client->request('GET', '/work-shop/privacy-policy');
        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();

        foreach (['Playground', 'Turnstile', 'Cloudflare'] as $unrelated) {
            static::assertStringNotContainsString($unrelated, $text, $unrelated);
        }
    }

    public function test_how_it_works_explains_what_surprises_buyers(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/blueprints/how-it-works');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        // The three things buyers get wrong, all of which have to appear before the CTA.
        static::assertStringContainsString('Checkout never asks for your GitHub account', $text);
        static::assertStringContainsString('Access is not sent automatically', $text);
        static::assertStringContainsString('Your email address is the only key', $text);
        static::assertStringContainsString('Merchant of Record', $text);
        static::assertStringContainsString('VAT ID', $text);
        static::assertSame(4, $crawler->filter('main ol li')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/terms-of-sales"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/privacy-policy"]')->count());
    }

    public function test_blueprint_listing_links_to_how_it_works(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/blueprints/symfony-backoffice');

        self::assertResponseIsSuccessful();
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/blueprints/how-it-works"]')->count());
    }

    public function test_header_does_not_link_to_work_shop(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/');

        self::assertResponseIsSuccessful();
        static::assertSame(0, $crawler->filter('header a[href^="/work-shop"]')->count());
    }

    public function test_work_shop_is_excluded_from_the_sitemap(): void
    {
        $client = self::createClient();
        $client->request('GET', '/sitemap.default.xml');

        self::assertResponseIsSuccessful();
        static::assertStringNotContainsString('/work-shop', (string) $client->getResponse()->getContent());
    }

    public function test_work_shop_index_renders_categories_and_listings(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('Blueprints', $text);
        static::assertStringContainsString('AI', $text);
        static::assertStringContainsString('Consulting', $text);
        static::assertStringNotContainsString('Subscriptions', $text);

        foreach ([
            '/work-shop/blueprints/symfony-backoffice',
            '/work-shop/ai/claude-skills',
            '/work-shop/consulting',
        ] as $href) {
            static::assertGreaterThan(0, $crawler->filter('a[href="' . $href . '"]')->count(), $href);
        }

        static::assertStringContainsString('$59', $text);
        static::assertStringContainsString('$10', $text);
        static::assertStringContainsString('excl. tax', $text);
        // Placeholder listings render as plain cards, so they must not become links.
        static::assertStringContainsString('More Blueprints', $text);
        static::assertGreaterThan(0, $crawler->filter('main div.card:not([href])')->count());
        static::assertGreaterThanOrEqual(2, $crawler->filter('main a[href^="/work-shop/"] img')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href^="/work-shop/"] ul li')->count());
    }

    public function test_success_page_explains_what_happens_next(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/success');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('Thank you', $text);
        static::assertStringContainsString('invitation', $text);
        // Access is not granted automatically, so the page has to say so before anything else.
        static::assertStringContainsString('Access is not sent automatically', $text);
        static::assertGreaterThan(0, $crawler->filter('main a[href="https://polar.sh/flow-php/portal"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main [data-controller~="work-shop-order"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main [data-controller~="work-shop-confetti"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href^="mailto:support@flow-php.com"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop"]')->count());
    }

    public function test_subscription_routes_are_gone(): void
    {
        self::createClient();

        $routes = type_instance_of(RouterInterface::class)
            ->assert(self::getContainer()->get('router'))
            ->getRouteCollection();

        foreach ([
            'work_shop_subscription_bronze',
            'work_shop_subscription_silver',
            'work_shop_subscription_gold',
        ] as $route) {
            static::assertNull($routes->get($route), $route);
        }
    }

    public function test_privacy_policy_renders_markdown_content(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/privacy-policy');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('Privacy Policy', $text);
        static::assertStringContainsString('GDPR', $text);
        static::assertGreaterThan(0, $crawler->filter('section#work-shop-privacy h2')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop"]')->count());
        // The counterpart legal page is linked from the template, so the markdown holds prose only.
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/terms-of-sales"]')->count());
    }

    public function test_terms_of_sales_renders_markdown_content(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/terms-of-sales');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('Terms of Sale and Use', $text);
        static::assertStringContainsString('Merchant of Record', $text);
        static::assertGreaterThan(0, $crawler->filter('section#work-shop-terms h2')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop"]')->count());
        // The counterpart legal page is linked from the template, so the markdown holds prose only.
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/privacy-policy"]')->count());
    }

    public function test_symfony_backoffice_cta_opens_embedded_checkout(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/blueprints/symfony-backoffice');

        self::assertResponseIsSuccessful();

        $cta = $crawler->filter(
            'main a[data-controller~="work-shop-checkout"][data-action~="work-shop-checkout#open"]',
        );
        static::assertSame(1, $cta->count());
        static::assertNotEmpty($cta->attr('data-work-shop-checkout-url-value'));
        static::assertSame($cta->attr('data-work-shop-checkout-url-value'), $cta->attr('href'));
    }

    public function test_ai_how_it_works_explains_what_surprises_buyers(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/ai/how-it-works');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        // The three things buyers get wrong, all of which have to appear before the CTA.
        static::assertStringContainsString('Checkout never asks for your GitHub account', $text);
        static::assertStringContainsString('Access is not sent automatically', $text);
        static::assertStringContainsString('Your email address is the only key', $text);
        static::assertStringContainsString('Merchant of Record', $text);
        static::assertStringContainsString('VAT ID', $text);
        // Delivery ends in an installed plugin, not a cloned repository.
        static::assertStringContainsString('/plugin marketplace add flow-php-depot/claude-skills', $text);
        static::assertSame(4, $crawler->filter('main ol li')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/ai/claude-skills"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/terms-of-sales"]')->count());
        static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/privacy-policy"]')->count());
    }

    public function test_claude_skills_cta_opens_embedded_checkout(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/ai/claude-skills');

        self::assertResponseIsSuccessful();

        $cta = $crawler->filter(
            'main a[data-controller~="work-shop-checkout"][data-action~="work-shop-checkout#open"]',
        );
        static::assertSame(1, $cta->count());
        static::assertNotEmpty($cta->attr('data-work-shop-checkout-url-value'));
        static::assertSame($cta->attr('data-work-shop-checkout-url-value'), $cta->attr('href'));
    }

    public function test_claude_skills_has_carousel_legal_links_and_covered_features(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/ai/claude-skills');

        self::assertResponseIsSuccessful();

        $text = $crawler->filter('main')->text();
        static::assertStringContainsString('$10', $text);
        static::assertStringContainsString('excl. tax', $text);
        static::assertStringContainsString('The task file is the plan', $text);
        static::assertSame(1, $crawler->filter('[data-controller~="work-shop-carousel"]')->count());
        static::assertGreaterThan(1, $crawler->filter('[data-work-shop-carousel-target="slide"]')->count());
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/ai/how-it-works"]')->count());
        // Something is sold here, so the Terms of Sale govern the purchase.
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/terms-of-sales"]')->count());
        static::assertSame(1, $crawler->filter('main a[href="/work-shop/privacy-policy"]')->count());
    }

    public function test_symfony_backoffice_has_carousel_and_covered_features(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/work-shop/blueprints/symfony-backoffice');

        self::assertResponseIsSuccessful();
        static::assertStringContainsString('$59', $crawler->filter('main')->text());
        static::assertStringContainsString('excl. tax', $crawler->filter('main')->text());
        static::assertStringContainsString('One schema, four jobs', $crawler->filter('main')->text());
        static::assertSame(1, $crawler->filter('[data-controller~="work-shop-carousel"]')->count());
        static::assertGreaterThan(1, $crawler->filter('[data-work-shop-carousel-target="slide"]')->count());
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
