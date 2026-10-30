<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Join\Condition;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Join\Condition;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function array_merge;
use function array_slice;
use function array_unique;
use function array_values;

final readonly class Any implements Condition
{
    /**
     * @var array<Condition>
     */
    private array $comparisons;

    public function __construct(Condition $comparison, Condition ...$comparisons)
    {
        $this->comparisons = array_merge([$comparison], $comparisons);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $comparisons = array_values($rebuild->all($data['comparisons'] ?? null, Condition::class));

        return new self(
            $comparisons[0] ?? throw new InvalidArgumentException(self::class . ' holds at least one comparison'),
            ...array_slice($comparisons, 1),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['comparisons' => array_values($this->comparisons)]);
    }

    public function compare(Rows $left, Rows $right): array
    {
        $result = null;

        foreach ($this->comparisons as $comparison) {
            $compared = $comparison->compare($left, $right);

            if ($result === null) {
                $result = $compared;

                continue;
            }

            foreach ($compared as $i => $met) {
                $result[$i] = $result[$i] || $met;
            }
        }

        return $result ?? [];
    }

    /**
     * @return array<Condition>
     */
    public function comparisons(): array
    {
        return $this->comparisons;
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        $columns = [];

        foreach ($this->comparisons as $comparison) {
            $columns[] = $comparison->left();
        }

        return array_unique(array_merge(...$columns));
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        $columns = [];

        foreach ($this->comparisons as $comparison) {
            $columns[] = $comparison->right();
        }

        return array_unique(array_merge(...$columns));
    }
}
