<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use ReflectionMethod;

use function base64_encode;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class MethodModel
{
    public function __construct(
        public string $name,
        public ParametersModel $parameters,
        public TypesModel $returnType,
        public ?string $docComment = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'name' => type_string(),
            'parameters' => type_array(),
            'return_type' => type_array(),
            'doc_comment' => type_optional(type_string()),
        ])->assert($data);

        /** @var array<array<string, mixed>> $parameters */
        $parameters = $data['parameters'];
        /** @var array<array<string, mixed>> $returnType */
        $returnType = $data['return_type'];

        return new self(
            $data['name'],
            ParametersModel::fromArray($parameters),
            TypesModel::fromArray($returnType),
            $data['doc_comment'],
        );
    }

    public static function fromReflection(ReflectionMethod $reflectionMethod): self
    {
        $returnTypeReflection = $reflectionMethod->getReturnType();
        $docComment = $reflectionMethod->getDocComment();

        return new self(
            $reflectionMethod->getShortName(),
            ParametersModel::fromMethodReflection($reflectionMethod),
            $returnTypeReflection !== null ? TypesModel::fromReflection($returnTypeReflection) : new TypesModel([]),
            $docComment !== false ? base64_encode($docComment) : null,
        );
    }

    /**
     * @return list<TypeModel>
     */
    public function referencedTypes(): array
    {
        return [...$this->returnType->types, ...$this->parameters->types()];
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'parameters' => $this->parameters->normalize(),
            'return_type' => $this->returnType->normalize(),
            'doc_comment' => $this->docComment,
        ];
    }
}
