<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Evaluation;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function array_keys;
use function count;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_equals;
use function range;
use function sprintf;

final readonly class Filter
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
        $resolved = [];

        foreach ($predicates as $predicate) {
            $function = $resolver->resolve($predicate, $schema);
            $resolver->assertResolved($function, $schema);
            (new ExpandingFunctions())->refuse($function, 'filter');

            // type_bare() keeps the gate blind to nullability - a null-propagating predicate declares
            // ?boolean, and a NULL row is dropped like a false one.
            if (!type_equals(type_bare($function->returns()), type_boolean())) {
                throw new InvalidArgumentException(sprintf(
                    'filter() requires a predicate returning boolean, "%s" returns "%s". '
                    . 'Use an explicit comparison, e.g. ->notEquals(lit(0)).',
                    $function::class,
                    $function->returns()->toString(),
                ));
            }

            $resolved[] = $function;
        }

        return new self($resolved);
    }

    /**
     * The rows every predicate is TRUE for: each is evaluated only on the rows the ones before it kept.
     */
    public function filter(Rows $rows, FlowContext $context): Rows
    {
        if ($this->predicates === []) {
            return $rows;
        }

        $kept = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);

        foreach ($this->predicates as $predicate) {
            if ($kept === []) {
                break;
            }

            $truths = array_keys(
                (new Selection($kept))
                    ->evaluate($predicate, $rows, $context)
                    ->physicals(),
                true,
                true,
            );

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
