<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Evaluation;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Conjuncts;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function array_keys;
use function array_values;
use function count;
use function Flow\Types\DSL\type_boolean;
use function range;

final readonly class Filter implements Describable
{
    /**
     * @param list<ScalarFunction> $predicates resolved against the bound schema
     */
    private function __construct(
        private array $predicates,
    ) {}

    /**
     * @throws InvalidArgumentException
     */
    public static function bind(Schema $schema, ScalarFunction ...$predicates): self
    {
        $resolver = new ReferenceResolver();
        $conjuncts = new Conjuncts();
        $resolved = [];

        foreach ($predicates as $predicate) {
            // a row an All keeps is TRUE for every conjunct, and each conjunct alone evaluates natively, an All does not
            foreach ($conjuncts->of($resolver->bind($predicate, $schema, 'filter', type_boolean())) as $conjunct) {
                $resolved[] = $conjunct;
            }
        }

        return new self($resolved);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(array_values($rebuild->all($data['predicates'] ?? null, ScalarFunction::class)));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['predicates' => $this->predicates]);
    }

    /**
     * The rows every predicate is TRUE for, in order; a source may have applied the ones it answers before.
     */
    public function filter(Rows $rows, Backend $backend): Rows
    {
        if ($this->predicates === []) {
            return $rows;
        }

        $kept = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);

        foreach ($this->predicates as $predicate) {
            if ($kept === []) {
                break;
            }

            $truths = array_keys((new Selection($kept))->truths($predicate, $rows, $backend), true, true);

            if (count($kept) === $rows->count()) {
                $kept = $truths;

                continue;
            }

            $next = [];

            foreach ($truths as $row) {
                $next[] = $kept[$row];
            }

            $kept = $next;
        }

        return count($kept) === $rows->count() ? $rows : $rows->gather($kept);
    }
}
