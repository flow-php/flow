<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundRunCodeTest extends EndToEndTestCase
{
    public function test_run_simple_flow_pipeline(): void
    {
        $client = self::navigateWithRetry('/playground');

        $this->waitForWasmReady($client);

        $this->setPlaygroundCode($client, <<<'PHP'
            <?php
            require 'vendor/autoload.php';
            use function Flow\ETL\DSL\{df, from_array, to_output};
            df()->read(from_array([['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']]))->write(to_output(truncate: false))->run();
            PHP);

        $client->executeScript('document.getElementById("action-run").click();');

        $client->waitForElementToContain('[data-playground-output-target="container"]', 'Alice', 10);

        static::assertStringContainsString(
            'Alice',
            $client->getCrawler()->filter('[data-playground-output-target="container"]')->text(),
        );
        static::assertStringContainsString(
            'Bob',
            $client->getCrawler()->filter('[data-playground-output-target="container"]')->text(),
        );
    }
}
