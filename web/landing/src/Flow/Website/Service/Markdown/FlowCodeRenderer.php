<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\ChildNodeRendererInterface;
use League\CommonMark\Renderer\NodeRendererInterface;
use League\CommonMark\Util\HtmlElement;

class FlowCodeRenderer implements NodeRendererInterface
{
    public function render(Node $node, ChildNodeRendererInterface $childRenderer)
    {
        if (!$node instanceof FencedCode) {
            throw new InvalidArgumentException('Incompatible node type: ' . $node::class);
        }

        $infoWords = $node->getInfoWords();
        $language = $infoWords[0] ?? '';
        $class = $language ? 'language-' . $language : 'language-plain';

        $attrs = $node->data->get('attributes', []);
        $attrs['class'] = isset($attrs['class']) ? $attrs['class'] . ' ' . $class : $class;

        // Escape the code content
        $escapedContent = htmlspecialchars($node->getLiteral(), ENT_NOQUOTES, 'UTF-8');

        // Render as <pre><code class="language-...">content</code></pre>
        $codeElement = new HtmlElement('code', $attrs, $escapedContent);

        // Render as <pre class="language-php" data-controller="syntax-highlight"><code class="language-...">content</code></pre>
        return new HtmlElement('pre', array_merge($attrs, ['data-controller' => 'syntax-highlight']), $codeElement);
    }
}
