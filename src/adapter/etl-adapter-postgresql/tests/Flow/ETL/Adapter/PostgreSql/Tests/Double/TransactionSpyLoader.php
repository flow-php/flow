<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\PostgreSql\Client\Client;
use SensitiveParameter;

final class TransactionSpyLoader implements Closure, Loader
{
    /** @var list<int> */
    public array $closureNestingLevels = [];

    /** @var list<array{rows: int, nestingLevel: int}> */
    public array $deliveries = [];

    public function __construct(
        private readonly Client $client,
    ) {}

    public function closure(): void
    {
        $this->closureNestingLevels[] = $this->client->getTransactionNestingLevel();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->deliveries[] = ['rows' => $rows->count(), 'nestingLevel' => $this->client->getTransactionNestingLevel()];
    }
}
