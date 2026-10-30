<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Transaction;

use function array_values;

final readonly class Transactional
{
    /**
     * @var list<Loader|Branch>
     */
    private array $writes;

    public function __construct(
        private Transaction $transaction,
        Loader|Branch ...$writes,
    ) {
        if ($writes === []) {
            throw new InvalidArgumentException('At least one write must be provided');
        }

        $this->writes = array_values($writes);
    }

    /**
     * @return list<Loader|Branch>
     */
    public function writes(): array
    {
        return $this->writes;
    }

    public function transaction(): Transaction
    {
        return $this->transaction;
    }
}
