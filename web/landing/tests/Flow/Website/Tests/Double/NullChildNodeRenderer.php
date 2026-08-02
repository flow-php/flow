<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Double;

use League\CommonMark\Renderer\ChildNodeRendererInterface;

final class NullChildNodeRenderer implements ChildNodeRendererInterface
{
    public function getBlockSeparator(): string
    {
        return "\n";
    }

    public function getInnerSeparator(): string
    {
        return "\n";
    }

    public function renderNodes(iterable $nodes): string
    {
        return '';
    }
}
