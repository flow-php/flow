<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Planning\Explain\RenderedNode;
use stdClass;

final class RenderedNodeMother
{
    /**
     * @param list<RenderedNode> $children
     * @param list<string> $lines
     */
    public static function named(
        string $name,
        ?int $number = 1,
        array $children = [],
        array $lines = [],
        string $suffix = '',
    ): RenderedNode {
        return new RenderedNode(new stdClass(), $name, $lines, $number, false, $children, $suffix);
    }
}
