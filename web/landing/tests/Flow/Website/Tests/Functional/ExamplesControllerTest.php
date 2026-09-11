<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

use function Flow\Types\DSL\type_string;

final class ExamplesControllerTest extends WebTestCase
{
    public function test_back_to_example_link_exists_on_playground_page(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/playground/caching/cache');

        self::assertResponseIsSuccessful();
        $link = $crawler->filter('a:contains("Back to Example")');
        static::assertCount(1, $link);
    }

    public function test_code_is_displayed_on_example_page(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/caching/cache/');

        self::assertResponseIsSuccessful();
        $code = $crawler->filter('#code-php');
        static::assertCount(1, $code);
        static::assertStringContainsString('<?php', $code->text());
    }

    public function test_example_option_page_returns_200(): void
    {
        $client = self::createClient();
        $client->request('GET', '/reading/csv/');

        self::assertResponseIsSuccessful();
        self::assertSelectorExists('a[href*="/playground/"]');
    }

    public function test_example_option_playground_page_returns_200(): void
    {
        $client = self::createClient();
        $client->request('GET', '/playground/reading/csv');

        self::assertResponseIsSuccessful();
        self::assertSelectorTextContains('h1', 'Playground');
    }

    public function test_example_page_displays_correct_title(): void
    {
        $client = self::createClient();
        $client->request('GET', '/caching/cache/');

        self::assertResponseIsSuccessful();
        self::assertSelectorTextContains('title', 'Flow PHP');
        self::assertSelectorTextContains('title', 'Cache');
    }

    public function test_example_page_returns_200(): void
    {
        $client = self::createClient();
        $client->request('GET', '/caching/cache/');

        self::assertResponseIsSuccessful();
        self::assertSelectorExists('a[href*="/playground/"]');
    }

    public function test_example_playground_page_returns_200(): void
    {
        $client = self::createClient();
        $client->request('GET', '/playground/caching/cache');

        self::assertResponseIsSuccessful();
        self::assertSelectorTextContains('h1', 'Playground');
        self::assertSelectorExists('a[href="/caching/cache/"]');
    }

    public function test_options_navigation_shows_on_example_with_options(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/reading/');

        self::assertResponseIsSuccessful();
        $options = $crawler->filter('a:contains("Csv")');
        static::assertGreaterThan(0, $options->count());
    }

    public function test_try_it_in_playground_link_exists_on_example_page(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/caching/cache/');

        self::assertResponseIsSuccessful();
        $link = $crawler->filter('a[title="Try in Playground"]');
        static::assertCount(1, $link);
        static::assertStringContainsString('/playground/', type_string()->assert($link->attr('href')));
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
