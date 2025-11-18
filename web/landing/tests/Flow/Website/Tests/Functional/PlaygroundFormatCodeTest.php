<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundFormatCodeTest extends EndToEndTestCase
{
    public function test_format_unformatted_code() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="formatButton"]', 3);

        $this->setPlaygroundCode($client, "<?php\ndf()->read(from_array([['id'=>1,'name'=>'Test']]))->run();");
        $client->waitForVisibility('[data-playground-target="formatButton"]', 3);
        $client->getCrawler()->filter('[data-playground-target="formatButton"]')->click();
        $client->wait(2);

        self::assertStringContainsString("'id' => 1", $this->getPlaygroundCode($client));
        self::assertStringNotContainsString("'id'=>1", $this->getPlaygroundCode($client));
    }
}
