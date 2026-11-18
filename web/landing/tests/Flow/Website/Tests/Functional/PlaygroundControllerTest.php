<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

final class PlaygroundControllerTest extends WebTestCase
{
    public function test_containers_with_user_data_are_masked(): void
    {
        $client = self::createClient();
        $crawler = $client->request('GET', '/playground');

        self::assertResponseIsSuccessful();

        foreach ([
            'select#file-select',
            '[data-playground-workspace-target="tree"]',
            '[data-playground-tabs-target="previewTab"]',
            '[data-playground-tabs-target="codePanel"]',
            '[data-playground-tabs-target="previewPanel"]',
            'pre#output',
        ] as $container) {
            static::assertCount(1, $crawler->filter($container . '.ph-no-capture'), $container);
        }
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
