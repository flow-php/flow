<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Operators\Transformer\Expansion;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Values\Schema;

use function Flow\Types\DSL\type_instance_of;

final class ExpansionContext
{
    public static function of(ScalarFunction $tree, ?Schema $input = null): Expansion
    {
        $schema = $input ?? ListColumnsMother::schema();

        return type_instance_of(Expansion::class)->assert(Expansion::of(
            (new ReferenceResolver())->resolve($tree, $schema),
            $schema,
        ));
    }
}
