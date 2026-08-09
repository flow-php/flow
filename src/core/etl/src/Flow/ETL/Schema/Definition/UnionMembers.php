<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\ETL\DSL\definition_from_type;

final readonly class UnionMembers
{
    /**
     * @param Definition<mixed> $definition
     */
    public function contains(UnionDefinition $union, Definition $definition): bool
    {
        foreach ($this->definitions($union) as $member) {
            if ($member->isCompatible($definition)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array<Definition<mixed>>
     */
    public function definitions(UnionDefinition $union): array
    {
        $definitions = [];

        foreach ($union->type()->types()->all() as $member) {
            try {
                $definitions[] = definition_from_type(
                    $union->entry(),
                    $member instanceof OptionalType ? $member->base() : $member,
                    nullable: true,
                );
            } catch (RuntimeException) {
                continue;
            }
        }

        return $definitions;
    }
}
