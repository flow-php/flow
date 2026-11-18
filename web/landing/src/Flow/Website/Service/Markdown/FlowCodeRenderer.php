<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\ChildNodeRendererInterface;
use League\CommonMark\Renderer\NodeRendererInterface;
use League\CommonMark\Util\HtmlElement;
use Symfony\Component\Asset\Packages;

use function array_key_exists;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

class FlowCodeRenderer implements NodeRendererInterface
{
    public function __construct(
        private readonly Packages $packages,
    ) {}

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

        $codeId = 'code-' . substr(hash('xxh3', $node->getLiteral()), 0, 12);

        return new HtmlElement(
            'div',
            ['class' => 'relative'],
            [
                new HtmlElement(
                    'button',
                    [
                        'class' => "absolute top-3 right-3 inline-flex items-center justify-center h-9 w-9 rounded-md border border-slate-300 bg-white hover:bg-slate-50 text-slate-700 [&.copied]:before:content-['Copied!'] before:absolute before:-translate-x-20 dark:border-white/10 dark:bg-black/60 dark:hover:bg-black/80 dark:text-white",
                        'data-controller' => 'clipboard',
                        'data-clipboard-kind-value' => 'documentation',
                        'data-clipboard-target' => '#' . $codeId,
                        'aria-label' => 'Copy code',
                    ],
                    new HtmlElement(
                        'img',
                        [
                            'src' => $this->packages->getUrl('images/icons/copy.svg'),
                            'alt' => '',
                            'width' => '16',
                            'height' => '16',
                            'class' => 'dark:invert-0 invert',
                        ],
                        selfClosing: true,
                    ),
                ),
                new HtmlElement(
                    'pre',
                    array_merge($attrs, ['id' => $codeId, 'data-controller' => 'syntax-highlight']),
                    new HtmlElement('code', $attrs, htmlspecialchars($node->getLiteral(), ENT_NOQUOTES, 'UTF-8')),
                ),
            ],
        );
    }
}
