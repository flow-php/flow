<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Rewrite;
use Flow\ETL\Tests\Mother\NodeMother;

final readonly class RenameSelectRewrite implements Rewrite
{
    public function of(Node $node): Node
    {
        return $node instanceof Select ? NodeMother::select($node->children()[0], 'name') : $node;
    }
}
