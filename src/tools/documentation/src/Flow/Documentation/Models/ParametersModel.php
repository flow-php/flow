<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\{type_array, type_list};

final class ParametersModel
{
    /**
     * @param array<ParameterModel> $arguments
     */
    public function __construct(
        public readonly array $arguments,
    ) {
    }

    /**
     * @param array<array<string, mixed>> $data
     */
    public static function fromArray(array $data) : self
    {
        type_list(type_array())->assert($data);

        return new self(
            array_map(static fn (array $argument) => ParameterModel::fromArray($argument), $data),
        );
    }

    public static function fromFunctionReflection(\ReflectionFunction $reflectionFunction) : self
    {
        $arguments = [];

        foreach ($reflectionFunction->getParameters() as $parameter) {
            $arguments[] = ParameterModel::fromReflection($parameter);
        }

        return new self($arguments);
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function normalize() : array
    {
        return array_map(fn (ParameterModel $argument) => $argument->normalize(), $this->arguments);
    }
}
