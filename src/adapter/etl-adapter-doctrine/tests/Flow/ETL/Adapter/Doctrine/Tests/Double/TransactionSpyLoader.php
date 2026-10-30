<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Double;

use Doctrine\DBAL\Connection;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

final class TransactionSpyLoader implements Closure, Loader
{
    /** @var list<bool> */
    public array $closureInTransaction = [];

    /** @var list<array{rows: int, inTransaction: bool}> */
    public array $deliveries = [];

    public function __construct(
        private readonly Connection $connection,
    ) {}

    public function closure(): void
    {
        $this->closureInTransaction[] = $this->connection->isTransactionActive();
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
        $this->deliveries[] = ['rows' => $rows->count(), 'inTransaction' => $this->connection->isTransactionActive()];
    }
}
