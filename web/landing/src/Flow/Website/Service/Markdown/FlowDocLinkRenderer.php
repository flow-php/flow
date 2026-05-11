<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Block\HtmlBlock;
use League\CommonMark\Node\Block\Paragraph;
use League\CommonMark\Node\Inline\Text;

final class FlowDocLinkRenderer
{
    public function __invoke(DocumentParsedEvent $event): void
    {
        $document = $event->getDocument();
        $walker = $document->walker();

        while ($walkEvent = $walker->next()) {
            if (!$walkEvent->isEntering()) {
                continue;
            }

            $node = $walkEvent->getNode();

            if (!$node instanceof Text) {
                continue;
            }

            if (!preg_match('/^\[DOC_LINK:(.+)\]$/', $node->getLiteral(), $m)) {
                continue;
            }

            $owner = $node->parent();

            if (!$owner instanceof Paragraph) {
                continue;
            }

            if ($owner->firstChild() !== $node || $node->next() !== null) {
                continue;
            }

            $href = $this->normaliseHref(trim($m[1]));

            $hrefAttr = htmlspecialchars($href, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
            $html = '<a href="' . $hrefAttr . '" class="back-link"><span aria-hidden="true">←</span> Back</a>';

            $block = new HtmlBlock(HtmlBlock::TYPE_6_BLOCK_ELEMENT);
            $block->setLiteral($html);
            $owner->insertBefore($block);
            $owner->detach();
        }
    }

    private function normaliseHref(string $href): string
    {
        $parts = parse_url($href);

        if (!isset($parts['scheme']) && !isset($parts['host'])) {
            $href = preg_replace('/\.md(?=$|[?#])/', '', $href);
        }

        return $href;
    }
}
