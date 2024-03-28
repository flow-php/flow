<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Matcher;

use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\SchemaMatcher;

final class StrictSchemaMatcher implements SchemaMatcher
{
    public function match(Schema $left, Schema $right) : bool
    {
        if (\count($left->definitions()) !== \count($right->definitions())) {
            return false;
        }

        foreach ($left->definitions() as $leftDefinition) {
            $rightDefinition = $right->findDefinition($leftDefinition->entry());

            if ($rightDefinition === null) {
                return false;
            }

            if (!$leftDefinition->isEqual($rightDefinition)) {
                return false;
            }
        }

        return true;
    }
}
