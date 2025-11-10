<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundShareCodeTest extends EndToEndTestCase
{
    public function test_load_code_from_url_parameter() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\necho 'Share Test';");
        $client->getCrawler()->filter('button[data-action*="share-code#share"]')->click();
        $client->wait(1);
        $this->dismissAlert($client);

        \parse_str(\parse_url($client->getCurrentURL(), \PHP_URL_QUERY), $params);

        $client->request('GET', '/playground?c=' . $params['c']);
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        self::assertStringContainsString('Share Test', $this->getPlaygroundCode($client));
    }

    public function test_share_code_generates_url_with_encoded_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-editor-target="runButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\necho 'test';");
        $client->getCrawler()->filter('button[data-action*="share-code#share"]')->click();
        $client->wait(1);
        $this->dismissAlert($client);

        self::assertStringContainsString('?c=', $client->getCurrentURL());
    }
}
