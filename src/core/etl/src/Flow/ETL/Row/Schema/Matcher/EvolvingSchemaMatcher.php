<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Matcher;

use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\SchemaMatcher;

final class EvolvingSchemaMatcher implements SchemaMatcher
{
    /**
     * Rules of evolving schema matching:
     * - if schemas are the same, return true
     * - if right schema has less fields than left schema, return false
     * - if right schema is making a nullable field non-nullable, return false
     * - if right schema is making a non-nullable field nullable, return true
     * - if right schema is changing the type of a field, return false
     * - if right schema is adding a field, return true
     */
    public function match(Schema $left, Schema $right) : bool
    {
        if ($right->count() < $left->count()) {
            return false;
        }

        foreach ($right->definitions() as $rightDefinition) {
            $leftDefinition = $left->findDefinition($rightDefinition->entry());

            if ($leftDefinition === null) {
                if ($right->count() === $left->count()) {
                    return false;
                }

                continue;
            }

            if (!$rightDefinition->isNullable() && $leftDefinition->isNullable()) {
                return false;
            }

            // making both sides nullable to compare just types of the fields
            if (!$rightDefinition->type()->makeNullable(true)->isEqual($leftDefinition->type()->makeNullable(true))) {
                return false;
            }
        }

        return true;
    }
}
