<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Transformer\Expansion;

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
