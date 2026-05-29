<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use ReflectionFunction;
use ReflectionMethod;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final readonly class ParametersModel
{
    /**
     * @param array<ParameterModel> $arguments
     */
    public function __construct(
        public array $arguments,
    ) {}

    /**
     * @param array<array<string, mixed>> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_list(type_map(type_string(), type_mixed()))->assert($data);

        return new self(array_map(ParameterModel::fromArray(...), $data));
    }

    public static function fromFunctionReflection(ReflectionFunction $reflectionFunction): self
    {
        $arguments = [];

        foreach ($reflectionFunction->getParameters() as $parameter) {
            $arguments[] = ParameterModel::fromReflection($parameter);
        }

        return new self($arguments);
    }

    public static function fromMethodReflection(ReflectionMethod $reflectionMethod): self
    {
        $arguments = [];

        foreach ($reflectionMethod->getParameters() as $parameter) {
            $arguments[] = ParameterModel::fromReflection($parameter);
        }

        return new self($arguments);
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function normalize(): array
    {
        return array_map(static fn(ParameterModel $argument) => $argument->normalize(), $this->arguments);
    }
}
