<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Sink;
use Flow\ETL\Transaction;

use function array_values;

/**
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
final readonly class Transactional implements Sink
{
    /**
     * @var list<Loader|Sink>
     */
    private array $sinks;

    public function __construct(
        private Transaction $transaction,
        Loader|Sink ...$sinks,
    ) {
        if ($sinks === []) {
            throw new InvalidArgumentException('At least one loader must be provided');
        }

        $this->sinks = array_values($sinks);
    }

    /**
     * @return Sinks
     */
    public function roots(DataFrame $prefix): array
    {
        $writes = [];

        foreach ($this->sinks as $sink) {
            foreach ((new Roots())->of($prefix, $sink) as $root) {
                // checked BEFORE the splat below, or PHP raises a TypeError first
                $writes[] = $root instanceof Node\Transaction
                    ? throw InvalidLogicException::nestedTransaction()
                    : $root;
            }
        }

        return [new Node\Transaction($this->transaction, ...$writes)];
    }
}
