<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use function array_merge;

final readonly class Conjuncts
{
    /**
     * @return list<ScalarFunction>
     */
    public function of(ScalarFunction $function): array
    {
        if (!$function instanceof All) {
            return [$function];
        }

        $conjuncts = [];

        foreach ($function->children() as $child) {
            $conjuncts[] = $this->of($child);
        }

        return array_merge(...$conjuncts);
    }
}
