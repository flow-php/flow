<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

final class DocumentationTest extends WebTestCase
{
    public function test_documentation_dsl_function_page() : void
    {
        $client = self::createClient();

        $client->request('GET', '/documentation/dsl/core/df');

        self::assertResponseIsSuccessful();
        self::assertSelectorExists('[data-dsl-function]');
        self::assertSelectorExists('[data-dsl-source-link]');
        self::assertStringContainsString('https://github.com', $client->getCrawler()->filter('[data-dsl-source-link]')->attr('href'));
        self::assertSelectorExists('pre');
        self::assertSelectorExists('code.language-php');
    }

    public function test_documentation_dsl_page() : void
    {
        $client = self::createClient();

        $client->request('GET', '/documentation/dsl');

        self::assertResponseIsSuccessful();
        self::assertGreaterThan(0, $client->getCrawler()->filter('[data-dsl-function]')->count());
        self::assertGreaterThan(0, $client->getCrawler()->filter('[data-dsl-source-link]')->count());
        self::assertEquals(15, $client->getCrawler()->filter('[data-dsl-module]')->count());
        self::assertEquals(11, $client->getCrawler()->filter('[data-dsl-type]')->count());
    }

    protected static function getKernelClass() : string
    {
        return Kernel::class;
    }
}
