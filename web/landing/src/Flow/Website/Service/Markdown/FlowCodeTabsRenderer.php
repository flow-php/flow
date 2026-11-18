<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Extension\CommonMark\Node\Block\HtmlBlock;
use League\CommonMark\Node\Node;

use function array_key_last;
use function htmlspecialchars;
use function preg_match;

final class FlowCodeTabsRenderer
{
    public function __invoke(DocumentParsedEvent $event): void
    {
        $walker = $event->getDocument()->walker();
        $groups = [];
        $group = -1;

        while ($walkEvent = $walker->next()) {
            $node = $walkEvent->getNode();

            if (!$walkEvent->isEntering() || !$node instanceof FencedCode || $this->tabName($node) === null) {
                continue;
            }

            if ($this->tabName($node->previous()) === null) {
                $group++;
            }

            $groups[$group][] = $node;
        }

        foreach ($groups as $blocks) {
            $tabs = '';

            foreach ($blocks as $block) {
                $name = htmlspecialchars((string) $this->tabName($block), ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
                $tabs .=
                    '<button type="button" role="tab" class="code-tab" data-code-tabs-target="tab" data-action="code-tabs#select" data-code-tabs-name-param="'
                    . $name
                    . '">'
                    . $name
                    . '</button>';

                $block->insertBefore($this->html(
                    '<div class="code-tabs-panel" role="tabpanel" data-code-tabs-target="panel" data-tab="'
                    . $name
                    . '"><p class="code-tabs-label">'
                    . $name
                    . '</p>',
                ));
                $block->insertAfter($this->html('</div>'));
            }

            $blocks[0]
                ->previous()
                ?->insertBefore($this->html(
                    '<div class="code-tabs" data-controller="code-tabs"><div class="code-tabs-list" role="tablist">'
                    . $tabs
                    . '</div>',
                ));
            $blocks[array_key_last($blocks)]->next()?->insertAfter($this->html('</div>'));
        }
    }

    private function html(string $literal): HtmlBlock
    {
        $block = new HtmlBlock(HtmlBlock::TYPE_6_BLOCK_ELEMENT);
        $block->setLiteral($literal);

        return $block;
    }

    private function tabName(?Node $node): ?string
    {
        $name = [];

        if (!$node instanceof FencedCode || preg_match('/\btab="([^"]+)"/', (string) $node->getInfo(), $name) !== 1) {
            return null;
        }

        return $name[1];
    }
}
