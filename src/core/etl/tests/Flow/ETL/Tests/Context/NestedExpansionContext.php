<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Transformer\NestedExpansion;

use function Flow\Types\DSL\type_instance_of;

final class NestedExpansionContext
{
    public static function of(ScalarFunction $tree, ?Schema $input = null): NestedExpansion
    {
        $schema = $input ?? ListColumnsMother::schema();

        return type_instance_of(NestedExpansion::class)->assert(NestedExpansion::of(
            (new ReferenceResolver())->resolve($tree, $schema),
            $schema,
        ));
    }
}
