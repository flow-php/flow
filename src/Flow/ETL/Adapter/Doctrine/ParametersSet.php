<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final class ParametersSet
{
    /**
     * @var array<array<string, mixed>|list<mixed>> $parameters
     */
    private array $parameters;

    /**
     * @param array<string, mixed>|list<mixed> ...$parameters
     */
    public function __construct(array ...$parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * @return array<array<string, mixed>|list<mixed>>
     */
    public function all() : array
    {
        return $this->parameters;
    }
}
