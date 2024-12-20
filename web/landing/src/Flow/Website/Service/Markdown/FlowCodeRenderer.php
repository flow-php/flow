<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\{ChildNodeRendererInterface, NodeRendererInterface};
use League\CommonMark\Util\HtmlElement;

class FlowCodeRenderer implements NodeRendererInterface
{
    public function render(Node $node, ChildNodeRendererInterface $childRenderer)
    {
        if (!$node instanceof FencedCode) {
            throw new \InvalidArgumentException('Incompatible node type: ' . $node::class);
        }

        $infoWords = $node->getInfoWords();
        $language = $infoWords[0] ?? '';
        $class = $language ? 'language-' . $language : 'language-plain';

        $attrs = $node->data->get('attributes', []);
        $attrs['class'] = isset($attrs['class']) ? $attrs['class'] . ' ' . $class : $class;

        // Escape the code content
        $escapedContent = htmlspecialchars($node->getLiteral(), ENT_NOQUOTES, 'UTF-8');

        // Render as <pre><code class="language-...">content</code></pre>
        $codeElement = new HtmlElement(
            'code',
            \array_merge(
                $attrs,
                [
                    'data-controller' => 'syntax-highlight',
                ]
            ),
            $escapedContent
        );

        return new HtmlElement('pre', $attrs, $codeElement);
    }
}
