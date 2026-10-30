<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Join;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Join\Condition\All;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Condition\Identical;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\HashJoin\JoinKeys;
use Flow\ETL\Kernels\HashJoin\PairPredicate;
use Flow\ETL\Kernels\Join\JoinSchema;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function array_keys;
use function array_map;
use function array_slice;
use function array_values;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_array;
use function is_string;

final readonly class Expression implements Describable, PairPredicate
{
    public function __construct(
        private Condition $comparison,
        private string $joinPrefix = '',
    ) {}

    /**
     * @param array<Condition>|array<string, string>|Condition $comparison
     */
    public static function on(array|Condition $comparison, string $joinPrefix = ''): self
    {
        if (is_array($comparison)) {
            /** @var array<Condition> $comparisons */
            $comparisons = [];

            foreach ($comparison as $left => $right) {
                if ($right instanceof Condition) {
                    $comparisons[] = $right;

                    continue;
                }

                if (!is_string($left)) {
                    throw new RuntimeException(
                        'Expected left column name to be string, got ' . gettype($left) . ". Example: ['id' => 'id']",
                    );
                }

                // @mago-ignore analysis:impossible-condition,redundant-type-comparison
                if (!is_string($right)) {
                    // @mago-ignore analysis:no-value
                    throw new RuntimeException(
                        'Expected right column name to be string, got ' . gettype($right) . ". Example: ['id' => 'id']",
                    );
                }

                $comparisons[] = new Equal($left, $right);
            }

            if ($comparisons === []) {
                throw new RuntimeException('Expected at least one comparison in the join expression.');
            }

            return new self(new All($comparisons[0], ...array_slice($comparisons, 1)), $joinPrefix);
        }

        return new self($comparison, $joinPrefix);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['comparison'] ?? null, Condition::class),
            type_string()->assert($data['join_prefix'] ?? null),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'comparison' => $this->comparison,
            'join_prefix' => $this->joinPrefix,
        ]);
    }

    public function comparison(): Condition
    {
        return $this->comparison;
    }

    /**
     * @return array<Reference>
     */
    public function left(): array
    {
        return $this->comparison->left();
    }

    /**
     * The equality pairs of a conjunction, each once, as column names; null when the comparison holds none (it is
     * then verified on every pair of rows).
     */
    public function keys(): ?JoinKeys
    {
        $pairs = [];

        foreach ($this->equalities($this->comparison) as [$leftRef, $rightRef]) {
            $pairs[$leftRef->base() . "\x00" . $rightRef->base()] = [$leftRef->base(), $rightRef->base()];
        }

        return $pairs === []
            ? null
            : new JoinKeys(
                array_map(static fn(array $pair): string => $pair[0], array_values($pairs)),
                array_map(static fn(array $pair): string => $pair[1], array_values($pairs)),
            );
    }

    /**
     * @return list<array{Reference, Reference}>
     */
    private function equalities(Condition $comparison): array
    {
        if ($comparison instanceof Equal || $comparison instanceof Identical) {
            return [[$comparison->left()[0], $comparison->right()[0]]];
        }

        if (!$comparison instanceof All) {
            return [];
        }

        $pairs = [];

        foreach ($comparison->comparisons() as $child) {
            // children without extractable equalities are verified on candidate pairs instead
            $pairs = [...$pairs, ...$this->equalities($child)];
        }

        return $pairs;
    }

    public function meet(Rows $left, array $leftIndices, Rows $right, array $rightIndices): array
    {
        if ($leftIndices === []) {
            return [];
        }

        return $this->comparison->compare(
            $left->select(...$this->names($this->left()))->gather($leftIndices),
            $right->select(...$this->names($this->right()))->gather($rightIndices),
        );
    }

    /**
     * @param array<Reference> $references
     *
     * @return list<string>
     */
    private function names(array $references): array
    {
        $names = [];

        foreach ($references as $reference) {
            $names[$reference->base()] = true;
        }

        return array_keys($names);
    }

    public function prefix(): string
    {
        return $this->joinPrefix;
    }

    /**
     * Without a prefix, a join column named alike on both sides appears once: from the right side of a right join,
     * from the left side otherwise.
     */
    public function schema(JoinType $type): JoinSchema
    {
        $duplicates = [];

        if ($this->joinPrefix === '') {
            foreach ($this->left() as $leftRef) {
                foreach ($this->right() as $rightRef) {
                    if ($leftRef->name() === $rightRef->name()) {
                        $duplicates[] = $leftRef->name();

                        continue 2;
                    }
                }
            }
        }

        return new JoinSchema(
            $this->joinPrefix,
            $type === JoinType::right ? $duplicates : [],
            $type === JoinType::right ? [] : $duplicates,
            $type,
        );
    }

    /**
     * @return array<Reference>
     */
    public function right(): array
    {
        return $this->comparison->right();
    }
}
