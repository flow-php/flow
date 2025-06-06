<?php

declare(strict_types=1);

namespace Flow\Website\Service\Markdown;

use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Node;
use League\CommonMark\Renderer\{ChildNodeRendererInterface, NodeRendererInterface};
use League\CommonMark\Util\HtmlElement;
use Symfony\Component\Asset\Packages;

final class MermaidCodeRenderer implements NodeRendererInterface
{
    public function __construct(private readonly Packages $packages)
    {
    }

    public function render(Node $node, ChildNodeRendererInterface $childRenderer)
    {
        if (!($node instanceof FencedCode)) {
            throw new \InvalidArgumentException('Incompatible node type: ' . $node::class);
        }

        $info = $node->getInfo();
        $literal = $node->getLiteral();

        if ($info === 'mermaid') {
            return new HtmlElement(
                'div',
                ['class' => 'mermaid-wrapper', 'data-controller' => 'mermaid'],
                $this->renderElements([
                    new HtmlElement(
                        'div',
                        ['class' => 'navigation'],
                        $this->renderElements([
                            new HTMLElement(
                                'button',
                                ['class' => 'button', 'data-mermaid-target' => 'zoomIn'],
                                $this->renderElements([
                                    new HtmlElement('img', ['src' => $this->packages->getUrl('images/icons/zoom-in.svg')], selfClosing: true),
                                ])
                            ),
                            new HTMLElement(
                                'button',
                                ['class' => 'button', 'data-mermaid-target' => 'zoomOut'],
                                $this->renderElements([
                                    new HtmlElement('img', ['src' => $this->packages->getUrl('images/icons/zoom-out.svg')], selfClosing: true),
                                ])
                            ),
                        ]),
                    ),
                    new HtmlElement('pre', ['class' => 'mermaid', 'data-mermaid-target' => 'svg'], $literal),
                ])
            );
        }

        return null;
    }

    /**
     * @param array<HtmlElement> $elements
     */
    private function renderElements(array $elements) : string
    {
        return implode('', array_map(static fn (HtmlElement $element) : string => $element->__toString(), $elements));
    }
}
