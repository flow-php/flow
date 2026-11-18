<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function basename;
use function dirname;
use function file_get_contents;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function glob;
use function preg_match_all;
use function str_contains;
use function trim;

final class GuidesCodeTest extends EndToEndTestCase
{
    public static function guides(): Generator
    {
        foreach (type_list(type_string())->assert(glob(dirname(__DIR__, 5) . '/content/guides/*.md')) as $guide) {
            if (str_contains(type_string()->assert(file_get_contents($guide)), "```php\n")) {
                yield basename($guide, '.md') => [$guide];
            }
        }
    }

    #[DataProvider('guides')]
    public function test_every_php_block_of_a_guide_runs_in_the_playground(string $guide): void
    {
        $blocks = [];
        preg_match_all(
            '/^```(php|text output)\n(.*?)^```$/ms',
            type_string()->assert(file_get_contents($guide)),
            $blocks,
            PREG_SET_ORDER,
        );

        static::assertNotSame([], $blocks);

        $page = $this->pageOf($this->openPlayground('/playground'));
        $output = '';

        foreach ($blocks as [, $kind, $content]) {
            if ($kind === 'text output') {
                static::assertStringContainsString(trim($content), $output);

                continue;
            }

            $this->setPlaygroundCode($page, $content);
            $page->evaluate('() => document.getElementById("action-run").click()');
            $page->waitForFunction('() => !document.getElementById("action-run").disabled');

            static::assertSame(
                '',
                type_string()->assert($page->evaluate(
                    '() => Array.from(document.querySelectorAll(\'[data-playground-output-target="container"] .output-error\')).map(e => e.textContent).join("\n")',
                )),
                $content,
            );

            $output = type_string()->assert($page->evaluate(
                '() => document.querySelector(\'[data-playground-output-target="container"]\').innerText',
            ));
        }
    }
}
