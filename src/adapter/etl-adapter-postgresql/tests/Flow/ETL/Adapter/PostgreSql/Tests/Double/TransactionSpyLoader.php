<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;
use Flow\PostgreSql\Client\Client;

final class TransactionSpyLoader implements Closure, Loader
{
    /** @var list<int> */
    public array $closureNestingLevels = [];

    /** @var list<array{rows: int, nestingLevel: int}> */
    public array $deliveries = [];

    public function __construct(
        private readonly Client $client,
    ) {}

    public function closure(FlowContext $context): void
    {
        $this->closureNestingLevels[] = $this->client->getTransactionNestingLevel();
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->deliveries[] = ['rows' => $rows->count(), 'nestingLevel' => $this->client->getTransactionNestingLevel()];
    }
}
