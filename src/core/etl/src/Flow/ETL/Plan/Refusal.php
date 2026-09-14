<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Exception\SchemaNotDerivableException;

final readonly class Refusal
{
    public function __construct(
        public SchemaNotDerivableException $exception,
    ) {}

    public static function of(SchemaNotDerivableException $e): self
    {
        return new self($e);
    }

    public function toException(): SchemaNotDerivableException
    {
        return $this->exception;
    }
}
