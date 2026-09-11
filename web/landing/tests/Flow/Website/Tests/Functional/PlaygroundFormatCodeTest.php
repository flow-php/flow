<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PlaygroundFormatCodeTest extends EndToEndTestCase
{
    public function test_format_unformatted_code(): void
    {
        $browser = $this->openPlayground('/playground');
        $page = $this->pageOf($browser);

        $this->setPlaygroundCode($page, "<?php\ndf()->read(from_array([['id'=>1,'name'=>'Test']]))->run();");

        $page->evaluate('() => document.getElementById("action-format").click()');
        $browser->waitUntilSeeIn('[data-playground-output-target="container"]', 'formatted');

        static::assertStringContainsString("'id' => 1", $this->getPlaygroundCode($page));
        static::assertStringNotContainsString("'id'=>1", $this->getPlaygroundCode($page));
    }
}
