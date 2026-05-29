<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\ChildNodeRendererInterface;
use League\CommonMark\Renderer\NodeRendererInterface;
use League\CommonMark\Util\HtmlElement;

use function array_key_exists;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

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

        $attrs = type_map(type_string(), type_union(
            type_string(),
            type_list(type_string()),
            type_boolean(),
        ))->assert($node->data->get('attributes', []));
        $attrs['class'] = array_key_exists('class', $attrs)
            ? type_string()->assert($attrs['class']) . ' ' . $class
            : $class;

        // Escape the code content
        $escapedContent = htmlspecialchars($node->getLiteral(), ENT_NOQUOTES, 'UTF-8');

        // Render as <pre><code class="language-...">content</code></pre>
        $codeElement = new HtmlElement('code', $attrs, $escapedContent);

        // Render as <pre class="language-php" data-controller="syntax-highlight"><code class="language-...">content</code></pre>
        return new HtmlElement('pre', array_merge($attrs, ['data-controller' => 'syntax-highlight']), $codeElement);
    }
}
