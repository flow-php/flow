<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundRunCodeTest extends EndToEndTestCase
{
    public function test_run_simple_flow_pipeline(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            require 'vendor/autoload.php';
            use function Flow\ETL\DSL\{df, from_array, to_output};
            df()->read(from_array([['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']]))->write(to_output(truncate: false))->run();
            PHP);

        $page->evaluate('() => document.getElementById("action-run").click()');

        $browser
            ->waitUntilSeeIn('[data-playground-output-target="container"]', 'Alice')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Alice')
            ->assertSeeIn('[data-playground-output-target="container"]', 'Bob');
    }
}
