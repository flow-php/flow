<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use function Flow\Types\DSL\type_string;

/**
 * A branched sink runs its pipeline in a Fiber, which the WASM build only supports through wasm/ucontext-emscripten.c.
 */
final class PlaygroundBranchSinkTest extends EndToEndTestCase
{
    public function test_branched_sinks_receive_every_batch_in_the_playground(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, <<<'PHP'
            <?php
            require 'vendor/autoload.php';
            use function Flow\ETL\DSL\{data_frame, from_sequence_number, ref, to_array, to_branch};
            $even = [];
            $odd = [];
            data_frame()
                ->read(from_sequence_number('id', 1, 9))
                ->batchSize(3)
                ->write(to_branch(ref('id')->isEven(), to_array($even)))
                ->write(to_branch(ref('id')->isOdd(), to_array($odd)))
                ->run();
            echo 'even=' . implode(',', array_column($even, 'id')) . ';odd=' . implode(',', array_column($odd, 'id')) . ';';
            PHP);

        $page->evaluate('() => document.getElementById("action-run").click()');

        $browser->waitUntilVisible('[data-playground-output-target="container"] .output-message');

        static::assertSame(
            '',
            type_string()->assert($page->evaluate(
                '() => Array.from(document.querySelectorAll(\'[data-playground-output-target="container"] .output-error\')).map(e => e.textContent).join("\n")',
            )),
        );
        $browser->assertSeeIn('[data-playground-output-target="container"]', 'even=2,4,6,8;odd=1,3,5,7,9;');
    }
}
