<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Exclusion;

use function array_values;

final readonly class AnyExclusionPolicy implements ExclusionPolicy
{
    /**
     * @var list<ExclusionPolicy>
     */
    private array $policies;

    public function __construct(ExclusionPolicy ...$policies)
    {
        $this->policies = array_values($policies);
    }

    public function exclude(SchemaObject $object): bool
    {
        foreach ($this->policies as $policy) {
            if ($policy->exclude($object)) {
                return true;
            }
        }

        return false;
    }
}
