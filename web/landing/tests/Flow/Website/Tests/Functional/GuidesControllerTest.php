<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

use function Flow\Types\DSL\type_string;
use function json_decode;

final class GuidesControllerTest extends WebTestCase
{
    public function test_guide_page(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/guides/read-large-csv-file-in-php');

        self::assertResponseIsSuccessful();
        static::assertSame(
            'Read a large CSV file in PHP without running out of memory | Flow PHP',
            $crawler->filter('title')->text(),
        );
        static::assertStringStartsWith(
            'A CSV file bigger than the memory limit ends with "Allowed memory size exhausted".',
            type_string()->assert($crawler->filter('meta[name="description"]')->attr('content')),
        );

        foreach ([
            'nav[aria-label="Breadcrumb"] a[href="/guides"]',
            'article#documentation-page[data-pagefind-body] h1',
            '[data-pagefind-filter="type:Guide"]',
            'time[datetime="2026-11-18"]',
            'header nav a[href="/guides"][aria-current="page"]',
            'a[href="https://github.com/flow-php/flow/edit/1.x/web/landing/content/guides/read-large-csv-file-in-php.md"]',
        ] as $element) {
            static::assertCount(1, $crawler->filter($element), $element);
        }

        static::assertGreaterThanOrEqual(
            4,
            $crawler->filter('article button[data-clipboard-kind-value="documentation"]')->count(),
        );

        static::assertSame(
            [
                [
                    '@context' => 'https://schema.org',
                    '@type' => 'Article',
                    'headline' => 'Read a large CSV file in PHP without running out of memory',
                    'description' => $crawler->filter('meta[name="description"]')->attr('content'),
                    'datePublished' => '2026-11-18',
                    'dateModified' => '2026-11-18',
                    'mainEntityOfPage' => 'http://localhost/guides/read-large-csv-file-in-php',
                ],
                [
                    '@context' => 'https://schema.org',
                    '@type' => 'BreadcrumbList',
                    'itemListElement' => [
                        [
                            '@type' => 'ListItem',
                            'position' => 1,
                            'name' => 'Guides',
                            'item' => 'http://localhost/guides',
                        ],
                        [
                            '@type' => 'ListItem',
                            'position' => 2,
                            'name' => 'Read a large CSV file in PHP without running out of memory',
                            'item' => 'http://localhost/guides/read-large-csv-file-in-php',
                        ],
                    ],
                ],
            ],
            json_decode(
                $crawler->filter('script[type="application/ld+json"]')->text(),
                true,
                flags: JSON_THROW_ON_ERROR,
            ),
        );
    }

    public function test_hub_lists_guides_by_group(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/guides');

        self::assertResponseIsSuccessful();
        static::assertSame('Guides | Flow PHP', $crawler->filter('title')->text());
        static::assertSame('Data processing in PHP', $crawler->filter('main h1')->text());
        static::assertSame('Reading files', $crawler->filter('main h2')->text());
        static::assertSame(
            'Read a large CSV file in PHP without running out of memory',
            $crawler->filter('main a[href="/guides/read-large-csv-file-in-php"]')->text(),
        );
    }

    public function test_sitemap_lists_guides(): void
    {
        $client = self::createClient();
        $client->request('GET', '/sitemap.guides.xml');

        self::assertResponseIsSuccessful();
        static::assertStringContainsString(
            '/guides/read-large-csv-file-in-php</loc>',
            type_string()->assert($client->getResponse()->getContent()),
        );
    }

    public function test_unknown_guide_is_not_found(): void
    {
        self::createClient()->request('GET', '/guides/does-not-exist');

        self::assertResponseStatusCodeSame(404);
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
