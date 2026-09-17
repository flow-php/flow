<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader;
use Flow\ETL\Sink;
use Flow\ETL\Transaction;

use function array_values;

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
     * @return list<Loader|Sink>
     */
    public function sinks(): array
    {
        return $this->sinks;
    }

    public function transaction(): Transaction
    {
        return $this->transaction;
    }

    public function write(DataFrame $prefix): void
    {
        $prefix->load($this);
    }
}
