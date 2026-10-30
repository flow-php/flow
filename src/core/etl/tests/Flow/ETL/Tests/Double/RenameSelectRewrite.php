<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Tests\Mother\NodeMother;

final readonly class RenameSelectRewrite implements Rewrite
{
    public function of(Node $original, Node $rebuilt): Node
    {
        return $rebuilt instanceof Select ? NodeMother::select($rebuilt->children()[0], 'name') : $rebuilt;
    }
}
