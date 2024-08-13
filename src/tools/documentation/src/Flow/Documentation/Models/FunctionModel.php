<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

final class FunctionModel
{
    public function __construct(
        public readonly string $name,
        public readonly string $namespace,
        public readonly ParametersModel $parameters,
        public readonly TypesModel $returnType,
        public readonly AttributesModel $attributes,
        public readonly ?string $docComment = null,
    ) {

    }

    public static function fromArray(array $data) : self
    {
        return new self(
            $data['name'],
            $data['namespace'],
            ParametersModel::fromArray($data['parameters']),
            TypesModel::fromArray($data['return_type']),
            AttributesModel::fromArray($data['attributes']),
            $data['doc_comment']
        );
    }

    public static function fromReflection(\ReflectionFunction $reflectionFunction) : self
    {
        return new self(
            $reflectionFunction->getShortName(),
            $reflectionFunction->getNamespaceName(),
            ParametersModel::fromFunctionReflection($reflectionFunction),
            TypesModel::fromReflection($reflectionFunction->getReturnType()),
            AttributesModel::fromReflection($reflectionFunction),
            $reflectionFunction->getDocComment() ? \base64_encode($reflectionFunction->getDocComment()) : null,
        );
    }

    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'parameters' => $this->parameters->normalize(),
            'return_type' => $this->returnType->normalize(),
            'attributes' => $this->attributes->normalize(),
            'doc_comment' => $this->docComment,
        ];
    }
}
