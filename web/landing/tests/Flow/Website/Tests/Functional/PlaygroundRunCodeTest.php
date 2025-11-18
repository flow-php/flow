<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundRunCodeTest extends EndToEndTestCase
{
    public function test_run_simple_flow_pipeline() : void
    {
        $client = self::createE2EClient();
        $client->request('GET', '/playground');
        $client->waitForEnabled('[data-playground-target="runButton"]', 3);

        $this->setPlaygroundCode(
            $client,
            <<<'PHP'
<?php
require 'vendor/autoload.php';
use function Flow\ETL\DSL\{df, from_array, to_output};
df()->read(from_array([['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']]))->write(to_output(truncate: false))->run();
PHP
        );

        $client->getCrawler()->filter('[data-playground-target="runButton"]')->click();
        $client->waitForElementToContain('[data-playground-target="output"]', 'Alice', 5);

        self::assertStringContainsString('Alice', $client->getCrawler()->filter('[data-playground-target="output"]')->text());
        self::assertStringContainsString('Bob', $client->getCrawler()->filter('[data-playground-target="output"]')->text());
    }
}
