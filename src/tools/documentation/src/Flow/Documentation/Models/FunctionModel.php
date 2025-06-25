<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\{type_array, type_integer, type_optional, type_string, type_structure};
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

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'repository_path' => type_string(),
            'start_line_in_file' => type_integer(),
            'slug' => type_string(),
            'name' => type_string(),
            'namespace' => type_string(),
            'parameters' => type_array(),
            'return_type' => type_array(),
            'attributes' => type_array(),
            'doc_comment' => type_optional(type_string()),
        ])->assert($data);

        /** @phpstan-var array<array<string, mixed>> $parameters */
        $parameters = $data['parameters'];
        /** @phpstan-var array<array<string, mixed>> $returnType */
        $returnType = $data['return_type'];
        /** @phpstan-var array<array<string, mixed>> $attributes */
        $attributes = $data['attributes'];

        return new self(
            $data['repository_path'],
            $data['start_line_in_file'],
            $data['slug'],
            $data['name'],
            $data['namespace'],
            ParametersModel::fromArray($parameters),
            TypesModel::fromArray($returnType),
            AttributesModel::fromArray($attributes),
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

    /**
     * @return array<string, mixed>
     */
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
