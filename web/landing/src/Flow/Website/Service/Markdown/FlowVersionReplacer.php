<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Event\DocumentParsedEvent;
use League\CommonMark\Extension\CommonMark\Node\Block\{FencedCode, HtmlBlock};
use League\CommonMark\Extension\CommonMark\Node\Inline\{Code, HtmlInline};
use League\CommonMark\Node\Inline\Text;

final class FlowVersionReplacer
{
    private const VERSION_PLACEHOLDER = '--FLOW_PHP_VERSION--';

    public function __construct(private readonly string $flowVersion)
    {
    }

    public function __invoke(DocumentParsedEvent $event) : void
    {
        $walker = $event->getDocument()->walker();

        while ($event = $walker->next()) {
            $node = $event->getNode();

            if ($event->isEntering()) {
                if ($node instanceof Code || $node instanceof Text || $node instanceof FencedCode) {
                    $this->replaceInLiteral($node);
                } elseif ($node instanceof HtmlInline || $node instanceof HtmlBlock) {
                    $this->replaceInHtml($node);
                }
            }
        }
    }

    private function replaceInHtml($node) : void
    {
        $html = $node->getLiteral();

        if (str_contains($html, self::VERSION_PLACEHOLDER)) {
            $node->setLiteral(str_replace(
                self::VERSION_PLACEHOLDER,
                self::FLOW_VERSION,
                $html
            ));
        }
    }

    private function replaceInLiteral($node) : void
    {
        $literal = $node->getLiteral();

        if (str_contains($literal, self::VERSION_PLACEHOLDER)) {
            $node->setLiteral(str_replace(
                self::VERSION_PLACEHOLDER,
                $this->flowVersion,
                $literal
            ));
        }
    }
}
