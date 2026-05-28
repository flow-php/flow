<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use InvalidArgumentException;
use League\CommonMark\Extension\TableOfContents\Node\TableOfContents;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\ChildNodeRendererInterface;
use League\CommonMark\Renderer\NodeRendererInterface;
use League\CommonMark\Util\HtmlElement;

final class TableOfContentsRenderer implements NodeRendererInterface
{
    public function render(Node $node, ChildNodeRendererInterface $childRenderer): HtmlElement|string|null
    {
        if (!$node instanceof TableOfContents) {
            throw new InvalidArgumentException('Incompatible node type: ' . $node::class);
        }

        $tocContent = $childRenderer->renderNodes($node->children());

        $tocList = new HtmlElement('ul', ['class' => 'table-of-contents'], $tocContent);

        $arrow = new HtmlElement(
            'svg',
            [
                'class' => 'toc-arrow',
                'xmlns' => 'http://www.w3.org/2000/svg',
                'width' => '20',
                'height' => '20',
                'viewBox' => '0 0 24 24',
                'fill' => 'none',
                'stroke' => 'currentColor',
                'stroke-width' => '2',
                'stroke-linecap' => 'round',
                'stroke-linejoin' => 'round',
            ],
            new HtmlElement('polyline', ['points' => '6 9 12 15 18 9'], '', true),
        );

        $header = new HtmlElement(
            'div',
            ['class' => 'toc-header', 'data-action' => 'click->toc#toggle'],
            [new HtmlElement('span', [], 'Table of Contents'), $arrow],
        );

        $content = new HtmlElement('div', ['class' => 'toc-content'], $tocList);

        return new HtmlElement('div', ['class' => 'toc-wrapper', 'data-controller' => 'toc'], [$header, $content]);
    }
}
