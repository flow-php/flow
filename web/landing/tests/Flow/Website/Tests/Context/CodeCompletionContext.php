<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Context;

use Playwright\Page\PageInterface;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class CodeCompletionContext
{
    private const string LABELS = '() => [...document.querySelectorAll(".cm-tooltip-autocomplete .cm-completionLabel")].map(label => label.textContent)';

    public function __construct(
        private PageInterface $page,
    ) {}

    public function documentationOpenedByModifierClickOn(string $text, int $occurrence = 0): string
    {
        $this->page->evaluate(
            '() => { window.flowOpened = []; window.open = url => { window.flowOpened.push(url); }; }',
        );
        ['x' => $x, 'y' => $y] = $this->coordinatesOf($text, $occurrence);
        $this->page->keyboard()->down('ControlOrMeta');
        $this->page->mouse()->click($x, $y);
        $this->page->keyboard()->up('ControlOrMeta');

        return type_string()->assert($this->page->evaluate('() => window.flowOpened[0] ?? ""'));
    }

    public function hoverTooltipOver(string $text, int $occurrence = 0): string
    {
        ['x' => $x, 'y' => $y] = $this->coordinatesOf($text, $occurrence);
        $this->page->mouse()->move($x, $y);
        $this->page->waitForSelector('.cm-tooltip-hover');

        return type_string()->assert($this->page->evaluate(
            '() => document.querySelector(".cm-tooltip-hover").textContent',
        ));
    }

    /**
     * @return array{text: string, active: string}
     */
    public function signatureHint(): array
    {
        $this->page->waitForSelector('.cm-flow-signature');
        $this->settle();

        return type_structure(['text' => type_string(), 'active' => type_string()])->assert($this->page->evaluate(
            '() => { const hint = document.querySelector(".cm-flow-signature");'
            . ' return { text: hint.textContent, active: hint.querySelector(".cm-flow-active-parameter")?.textContent ?? "" }; }',
        ));
    }

    /**
     * @return array{x: float, y: float}
     */
    public function coordinatesOf(string $text, int $occurrence = 0): array
    {
        return type_structure(['x' => type_float(), 'y' => type_float()])->assert($this->page->evaluate(
            '([text, occurrence]) => { const walker = document.createTreeWalker(document.querySelector(".cm-content"), NodeFilter.SHOW_TEXT);'
            . ' let seen = 0; for (let node = walker.nextNode(); node; node = walker.nextNode()) { const index = node.textContent.indexOf(text);'
            . ' if (index === -1 || seen++ < occurrence) continue; const range = document.createRange(); range.setStart(node, index + 1); range.setEnd(node, index + 2);'
            . ' const rect = range.getBoundingClientRect(); return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 }; }'
            . ' throw new Error("text not found: " + text); }',
            [$text, $occurrence],
        ));
    }

    public function accept(string $key = 'Enter'): void
    {
        $this->settle();
        $this->page->keyboard()->press($key);
    }

    /**
     * @return list<string>
     */
    public function suggestionsAfterTyping(string $text): array
    {
        $this->type($text);
        $this->page->waitForFunction(
            '() => new Promise(resolve => { let previous = null; let stable = 0; const tick = () => {'
            . ' const labels = JSON.stringify('
            . self::LABELS
            . '());'
            . ' if (labels === previous && labels !== "[]") { stable++; } else { stable = 0; previous = labels; }'
            . ' if (stable >= 3) { resolve(true); } else { setTimeout(tick, 100); } }; tick(); })',
        );

        return type_list(type_string())->assert($this->page->evaluate(self::LABELS));
    }

    /**
     * @return list<string>
     */
    public function suggestionsSettledAfterTyping(string $text): array
    {
        $this->type($text);
        $this->settle();

        return type_list(type_string())->assert($this->page->evaluate(self::LABELS));
    }

    // Completion queries are debounced and CodeMirror ignores Enter for interactionDelay (75ms) after the list changes.
    public function settle(): void
    {
        $this->page->evaluate('() => new Promise(resolve => setTimeout(resolve, 300))');
    }

    public function type(string $text): void
    {
        $this->page->evaluate('() => document.querySelector(".cm-content").focus()');
        $this->page->keyboard()->press('ControlOrMeta+End');
        $this->settle();
        $this->page->keyboard()->type($text, ['delay' => 30]);
    }
}
