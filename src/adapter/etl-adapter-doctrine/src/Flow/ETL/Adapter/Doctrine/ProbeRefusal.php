<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Driver\Exception as DriverError;
use Exception;
use Throwable;

/**
 * DBAL's exception converters key on the SQLSTATE (PostgreSQL), the code (MySQL) and the message (SQLite), so a
 * refusal carries all three for DbalResultSchema to convert into what the read itself would throw.
 */
final class ProbeRefusal extends Exception implements DriverError
{
    public function __construct(
        string $message,
        int $code = 0,
        private readonly ?string $sqlState = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function getSQLState(): ?string
    {
        return $this->sqlState;
    }
}
