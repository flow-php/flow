<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Component\Routing\RouterInterface;

use function dirname;
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

    public function test_work_shop_pages_do_not_render_the_contributors_section(): void
    {
        $client = self::createClient();

        // The Work-Shop is a commercial offer by the maintainer, not a community effort.
        foreach ([
            '/work-shop',
            '/work-shop/blueprints/symfony-backoffice',
            '/work-shop/blueprints/how-it-works',
            '/work-shop/ai/claude-skills',
            '/work-shop/ai/how-it-works',
            '/work-shop/sponsoring/1-month',
            '/work-shop/consulting',
            '/work-shop/success',
            '/work-shop/terms-of-sales',
            '/work-shop/privacy-policy',
        ] as $path) {
            $crawler = $client->request('GET', $path);

            self::assertResponseIsSuccessful();
            static::assertStringNotContainsString('Built in the open', $crawler->filter('main')->text(), $path);
        }

        // Everywhere else the section stays.
        $home = $client->request('GET', '/');
        self::assertResponseIsSuccessful();
        static::assertStringContainsString('Built in the open', $home->filter('main')->text());
    }

    public function test_header_links_to_the_work_shop_from_every_page(): void
    {
        $client = self::createClient();

        foreach (['/', '/sponsor', '/work-shop'] as $path) {
            $crawler = $client->request('GET', $path);

            self::assertResponseIsSuccessful();
            static::assertGreaterThan(0, $crawler->filter('header a[href="/work-shop"]')->count(), $path);
        }
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
        static::assertStringContainsString('Sponsoring', $text);
        static::assertStringContainsString('Consulting', $text);
        static::assertStringNotContainsString('Subscriptions', $text);

        foreach ([
            '/work-shop/blueprints/symfony-backoffice',
            '/work-shop/ai/claude-skills',
            '/work-shop/sponsoring/1-month',
            '/work-shop/sponsoring/6-months',
            '/work-shop/sponsoring/12-months',
            '/work-shop/consulting',
        ] as $href) {
            static::assertGreaterThan(0, $crawler->filter('a[href="' . $href . '"]')->count(), $href);
        }

        static::assertStringContainsString('$59', $text);
        static::assertStringContainsString('$10', $text);
        static::assertStringContainsString('excl. tax', $text);
        // The longer sponsoring periods advertise their discount, and 6 months is the pick.
        static::assertStringContainsString('save 3%', $text);
        static::assertStringContainsString('save 8%', $text);
        static::assertStringContainsString('Best value', $text);
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

    public function test_sponsoring_pages_sell_each_period(): void
    {
        $client = self::createClient();

        $pages = [
            '/work-shop/sponsoring/1-month' => ['$5', 'per month'],
            '/work-shop/sponsoring/6-months' => ['$29', 'every 6 months'],
            '/work-shop/sponsoring/12-months' => ['$55', 'every 12 months'],
        ];

        foreach ($pages as $path => [$price, $cadence]) {
            $crawler = $client->request('GET', $path);

            self::assertResponseIsSuccessful();

            $text = $crawler->filter('main')->text();
            static::assertStringContainsString($price, $text, $path);
            static::assertStringContainsString($cadence, $text, $path);
            static::assertStringContainsString('excl. tax', $text, $path);
            static::assertStringContainsString('Sponsor role', $text, $path);
            static::assertStringContainsString('custom username color', $text, $path);
            // Claiming the role requires connecting Discord in the portal, so the page must say so.
            static::assertStringContainsString('The role is not sent automatically', $text, $path);
            // Cancellation must be described exactly as Polar implements it: at period end, reversible.
            static::assertStringContainsString(
                'stay active until the end of the period you already paid for',
                $text,
                $path,
            );

            $cta = $crawler->filter(
                'main a[data-controller~="work-shop-checkout"][data-action~="work-shop-checkout#open"]',
            );
            static::assertSame(1, $cta->count(), $path);
            static::assertNotEmpty($cta->attr('data-work-shop-checkout-url-value'), $path);

            // Each period cross-links the other two.
            foreach (array_keys($pages) as $other) {
                if ($other === $path) {
                    continue;
                }
                static::assertGreaterThan(
                    0,
                    $crawler->filter('main a[href="' . $other . '"]')->count(),
                    $path . ' -> ' . $other,
                );
            }

            // Something is sold here, so the Terms of Sale govern the purchase.
            static::assertSame(1, $crawler->filter('main a[href="/work-shop/terms-of-sales"]')->count(), $path);
            static::assertSame(1, $crawler->filter('main a[href="/work-shop/privacy-policy"]')->count(), $path);
            // Sponsoring is not a support contract, so it has to point at consulting instead.
            static::assertGreaterThan(0, $crawler->filter('main a[href="/work-shop/consulting"]')->count(), $path);
        }
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

    public function test_listing_pages_generate_social_cards_and_point_open_graph_image_to_them(): void
    {
        $client = self::createClient();

        $pages = [
            '/work-shop/blueprints/symfony-backoffice' => '/images/work-shop/social/symfony-backoffice.png',
            '/work-shop/ai/claude-skills' => '/images/work-shop/social/claude-skills.png',
            '/work-shop/sponsoring/1-month' => '/images/work-shop/social/sponsoring-1-month.png',
            '/work-shop/sponsoring/6-months' => '/images/work-shop/social/sponsoring-6-months.png',
            '/work-shop/sponsoring/12-months' => '/images/work-shop/social/sponsoring-12-months.png',
            '/work-shop/consulting' => '/images/work-shop/social/consulting.png',
        ];

        foreach ($pages as $path => $socialCardPath) {
            $crawler = $client->request('GET', $path);

            self::assertResponseIsSuccessful();
            static::assertStringEndsWith(
                $socialCardPath,
                (string) $crawler->filter('meta[property="og:image"]')->attr('content'),
                $path,
            );
            static::assertStringEndsWith(
                $socialCardPath,
                (string) $crawler->filter('meta[name="twitter:image"]')->attr('content'),
                $path,
            );
            static::assertFileExists(dirname(__DIR__, levels: 5) . '/public' . $socialCardPath, $path);
        }
    }

    public function test_non_listing_work_shop_pages_keep_the_default_social_image(): void
    {
        $client = self::createClient();

        foreach (['/work-shop', '/work-shop/blueprints/how-it-works', '/work-shop/ai/how-it-works'] as $path) {
            $crawler = $client->request('GET', $path);

            self::assertResponseIsSuccessful();
            // The default banner is served through the asset mapper, so its filename carries a content hash.
            static::assertMatchesRegularExpression(
                '#/assets/images/banner-[0-9a-f]+\.png$#',
                (string) $crawler->filter('meta[property="og:image"]')->attr('content'),
                $path,
            );
        }
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
