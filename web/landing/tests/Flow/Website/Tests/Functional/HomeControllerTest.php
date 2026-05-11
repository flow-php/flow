<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

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

    #[\Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
