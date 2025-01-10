<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use Cocur\Slugify\Slugify;

final class FunctionModel
{
    public function __construct(
        public readonly string $repositoryPath,
        public readonly int|false $startLineInFile,
        public readonly string $slug,
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
            $data['repository_path'],
            $data['start_line_in_file'],
            $data['slug'],
            $data['name'],
            $data['namespace'],
            ParametersModel::fromArray($data['parameters']),
            TypesModel::fromArray($data['return_type']),
            AttributesModel::fromArray($data['attributes']),
            $data['doc_comment']
        );
    }

    public static function fromReflection(string $relativePath, \ReflectionFunction $reflectionFunction) : self
    {
        $returnTypeReflection = $reflectionFunction->getReturnType();

        if ($returnTypeReflection === null) {
            throw new \InvalidArgumentException('ReflectionType must be instance of ReflectionNamedType');
        }

        return new self(
            $relativePath,
            $reflectionFunction->getStartLine(),
            (new Slugify())->slugify($reflectionFunction->getShortName()),
            $reflectionFunction->getShortName(),
            $reflectionFunction->getNamespaceName(),
            ParametersModel::fromFunctionReflection($reflectionFunction),
            TypesModel::fromReflection($returnTypeReflection),
            AttributesModel::fromReflection($reflectionFunction),
            $reflectionFunction->getDocComment() ? \base64_encode($reflectionFunction->getDocComment()) : null,
        );
    }

    public function normalize() : array
    {
        return [
            'repository_path' => $this->repositoryPath,
            'start_line_in_file' => $this->startLineInFile,
            'slug' => $this->slug,
            'name' => $this->name,
            'namespace' => $this->namespace,
            'parameters' => $this->parameters->normalize(),
            'return_type' => $this->returnType->normalize(),
            'attributes' => $this->attributes->normalize(),
            'doc_comment' => $this->docComment,
        ];
    }
}
