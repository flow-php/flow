<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\{type_array, type_boolean, type_integer, type_optional, type_string, type_structure};
use Flow\ETL\Function\ScalarFunctionChain;
use Symfony\Component\String\Slugger\AsciiSlugger;

final readonly class MethodModel
{
    public function __construct(
        public string $repositoryPath,
        public int|false $startLineInFile,
        public string $slug,
        public string $name,
        public string $class,
        public string $classSlug,
        public ParametersModel $parameters,
        public TypesModel $returnType,
        public AttributesModel $attributes,
        public bool $scalarFunctionChain,
        public ?string $docComment = null,
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
            'class' => type_string(),
            'class_slug' => type_string(),
            'parameters' => type_array(),
            'return_type' => type_array(),
            'attributes' => type_array(),
            'scalar_function_chain' => type_boolean(),
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
            $data['class'],
            $data['class_slug'],
            ParametersModel::fromArray($parameters),
            TypesModel::fromArray($returnType),
            AttributesModel::fromArray($attributes),
            $data['scalar_function_chain'],
            $data['doc_comment']
        );
    }

    public static function fromReflection(string $relativePath, \ReflectionMethod $reflectionMethod) : self
    {
        $returnTypeReflection = $reflectionMethod->getReturnType();
        $declaringClass = $reflectionMethod->getDeclaringClass();
        $className = $declaringClass->getName();
        $classSlug = (new AsciiSlugger())->slug($declaringClass->getShortName())->lower()->toString();

        return new self(
            $relativePath,
            $reflectionMethod->getStartLine(),
            (new AsciiSlugger())->slug($reflectionMethod->getShortName())->lower()->toString(),
            $reflectionMethod->getShortName(),
            $className,
            $classSlug,
            ParametersModel::fromMethodReflection($reflectionMethod),
            $returnTypeReflection !== null ? TypesModel::fromReflection($returnTypeReflection) : new TypesModel([]),
            AttributesModel::fromReflection($reflectionMethod),
            $returnTypeReflection !== null ? self::isScalarFunctionChain($returnTypeReflection) : false,
            $reflectionMethod->getDocComment() ? \base64_encode($reflectionMethod->getDocComment()) : null,
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
            'class' => $this->class,
            'class_slug' => $this->classSlug,
            'parameters' => $this->parameters->normalize(),
            'return_type' => $this->returnType->normalize(),
            'attributes' => $this->attributes->normalize(),
            'scalar_function_chain' => $this->scalarFunctionChain,
            'doc_comment' => $this->docComment,
        ];
    }

    private static function isScalarFunctionChain(\ReflectionType $reflectionType) : bool
    {
        if ($reflectionType instanceof \ReflectionNamedType) {
            $typeName = $reflectionType->getName();

            if (!\class_exists($typeName)) {
                return false;
            }

            return \is_a($typeName, ScalarFunctionChain::class, true);
        }

        if ($reflectionType instanceof \ReflectionUnionType || $reflectionType instanceof \ReflectionIntersectionType) {
            foreach ($reflectionType->getTypes() as $type) {
                if (self::isScalarFunctionChain($type)) {
                    return true;
                }
            }
        }

        return false;
    }
}
