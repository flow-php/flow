<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Service\Markdown;

use Flow\Website\Service\Markdown\MermaidCodeRenderer;
use Flow\Website\Tests\Double\NullChildNodeRenderer;
use InvalidArgumentException;
use League\CommonMark\Extension\CommonMark\Node\Block\FencedCode;
use League\CommonMark\Node\Block\Paragraph;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Asset\Packages;
use Symfony\Component\Asset\PathPackage;
use Symfony\Component\Asset\VersionStrategy\EmptyVersionStrategy;

final class MermaidCodeRendererTest extends TestCase
{
    public function buildRenderer(): MermaidCodeRenderer
    {
        return new MermaidCodeRenderer(new Packages(new PathPackage('/', new EmptyVersionStrategy())));
    }

    public function test_renders_wrapper_for_mermaid_fenced_code(): void
    {
        $node = new FencedCode(3, '`', 0);
        $node->setInfo('mermaid');
        $node->setLiteral("flowchart LR\n    A[App] --> B[Tracer]\n");

        static::assertSame(
            '<div class="mermaid-wrapper" data-controller="mermaid" data-mermaid-state="pending"'
            . ' data-mermaid-src-value="/mermaid/mermaid.min.js">'
            . '<div class="navigation">'
            . '<button class="button" data-mermaid-target="zoomIn"><img src="/images/icons/zoom-in.svg" /></button>'
            . '<button class="button" data-mermaid-target="zoomOut"><img src="/images/icons/zoom-out.svg" /></button>'
            . '</div>'
            . "<pre class=\"mermaid\" data-mermaid-target=\"svg\">flowchart LR\n    A[App] --> B[Tracer]\n</pre>"
            . '</div>',
            (string) $this->buildRenderer()->render($node, new NullChildNodeRenderer()),
        );
    }

    #[TestWith(['php'])]
    #[TestWith(['json'])]
    #[TestWith([''])]
    public function test_returns_null_for_non_mermaid_fenced_code(string $info): void
    {
        $node = new FencedCode(3, '`', 0);
        $node->setInfo($info);
        $node->setLiteral('echo "not a diagram";');

        static::assertNull($this->buildRenderer()->render($node, new NullChildNodeRenderer()));
    }

    public function test_throws_on_incompatible_node_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Incompatible node type: ' . Paragraph::class);

        $this->buildRenderer()->render(new Paragraph(), new NullChildNodeRenderer());
    }
}
