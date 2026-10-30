<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Comparison\ValueEquality;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;

final readonly class JoinKeys
{
    /**
     * @param list<string> $left
     * @param list<string> $right column names, pairwise with $left
     */
    public function __construct(
        public array $left,
        public array $right,
    ) {}

    /**
     * One key encoder per side, both keying in types().
     *
     * @return array{KeyEncoder, KeyEncoder}
     */
    public function encoders(?Schema $left, ?Schema $right): array
    {
        $types = $this->types($left, $right);

        return [new KeyEncoder($this->left, $types), new KeyEncoder($this->right, $types)];
    }

    /**
     * By position, the one type each key pair compares in, so a key of either side meets its equal on the other;
     * a pair without both schemas keys in its own types.
     *
     * @return array<int, Type<mixed>>
     */
    public function types(?Schema $left, ?Schema $right): array
    {
        $types = [];

        foreach ($this->left as $position => $name) {
            $leftDefinition = $left?->findDefinition($name);
            $rightDefinition = $right?->findDefinition($this->right[$position]);

            if ($leftDefinition !== null && $rightDefinition !== null) {
                $types[$position] = (new ValueEquality())->type($leftDefinition->type(), $rightDefinition->type());
            }
        }

        return $types;
    }
}
