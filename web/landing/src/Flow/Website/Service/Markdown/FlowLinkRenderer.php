<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Inline\Link;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\ChildNodeRendererInterface;
use League\CommonMark\Renderer\NodeRendererInterface;
use League\CommonMark\Util\HtmlElement;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

class FlowLinkRenderer implements NodeRendererInterface
{
    public function __construct() {}

    public function render(Node $node, ChildNodeRendererInterface $childRenderer)
    {
        if (!$node instanceof Link) {
            throw new InvalidArgumentException('Incompatible node type: ' . $node::class);
        }

        $attrs = type_map(type_string(), type_union(
            type_string(),
            type_list(type_string()),
            type_boolean(),
        ))->assert($node->data->get('attributes'));

        $urlParts = parse_url($node->getUrl());

        if (!isset($urlParts['scheme']) && !isset($urlParts['host'])) {
            $node->setUrl(str_replace('.md', '', $node->getUrl()));
        }

        if (str_starts_with($node->getUrl(), '/src')) {
            $node->setUrl('https://github.com/flow-php/flow/blob/1.x' . $node->getUrl());
            $attrs['target'] = '_blank';
        }

        $attrs['href'] = $node->getUrl();

        $title = $node->getTitle();

        if ($title !== null && $title !== '') {
            $attrs['title'] = $title;
        }

        return new HtmlElement('a', $attrs, $childRenderer->renderNodes($node->children()));
    }
}
