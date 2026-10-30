<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function array_values;
use function count;

final readonly class RowMerger
{
    /**
     * @param list<string> $left the left column each leading output column is gathered from
     * @param list<string> $right the right column each following output column is gathered from, renamed by $output
     */
    public function __construct(
        private Schema $output,
        private array $left,
        private array $right,
    ) {}

    /**
     * Pair-aligned: row i of the result is left row i merged with right row i, the columns in $output's order and
     * names, each keeping its own definition; a column a batch lacks is left out.
     */
    public function merge(Rows $left, Rows $right): Rows
    {
        $names = array_values($this->output->names());
        $leftCount = count($this->left);
        $definitions = [];
        $columns = [];

        foreach ([...$this->left, ...$this->right] as $position => $source) {
            $side = $position < $leftCount ? $left : $right;
            $definition = $side->schema()->findDefinition($source);

            if ($definition === null) {
                continue;
            }

            $definitions[] = $definition->rename($names[$position]);
            $columns[$names[$position]] = $side->column($source);
        }

        return Rows::fromColumns(new Schema(...$definitions), $columns, $left->count());
    }
}
