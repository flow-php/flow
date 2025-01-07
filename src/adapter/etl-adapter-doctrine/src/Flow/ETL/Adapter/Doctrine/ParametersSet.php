<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class ParametersSet
{
    /**
     * @var array<array<string, mixed>>|array<list<mixed>>
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
     * @return array<array<string, mixed>>|array<list<mixed>>
     */
    public function all() : array
    {
        return $this->parameters;
    }
}
