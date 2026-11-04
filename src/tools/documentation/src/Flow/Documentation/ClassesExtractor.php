<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\ClassModel;
use Flow\Documentation\Models\MethodModel;
use Generator;
use ReflectionClassConstant;
use ReflectionMethod;
use Roave\BetterReflection\Reflection\ReflectionEnum;
use Roave\BetterReflection\Reflector\Exception\IdentifierNotFound;
use Roave\BetterReflection\Reflector\Reflector;

use function array_filter;
use function array_key_exists;
use function array_keys;
use function array_merge;
use function array_pop;
use function array_values;
use function base64_encode;
use function in_array;
use function ltrim;
use function realpath;
use function str_contains;
use function str_replace;
use function str_starts_with;

final readonly class ClassesExtractor
{
    /**
     * @param list<string> $phpExtensions PHP extensions whose built-in types are extracted next to the repository ones
     */
    public function __construct(
        private string $repositoryRootPath,
        private Reflector $reflector,
        private array $phpExtensions = [],
    ) {}

    /**
     * @param list<string> $roots
     *
     * @return \Generator<ClassModel>
     */
    public function extract(array $roots): Generator
    {
        $queue = $roots;
        $extracted = [];

        while ($queue !== []) {
            $className = array_pop($queue);

            if (array_key_exists($className, $extracted) || !$this->isExtractedType($className)) {
                continue;
            }

            $extracted[$className] = true;
            $reflectionClass = $this->reflector->reflectClass($className);
            $parentClassName = $reflectionClass->getParentClassName();
            $supertypes = array_values(array_filter(
                array_merge(
                    $parentClassName !== null ? [$parentClassName] : [],
                    $reflectionClass->getInterfaceNames(),
                    $reflectionClass->getTraitNames(),
                ),
                $this->isExtractedType(...),
            ));
            $queue = array_merge($queue, $supertypes);
            $methods = [];
            $staticMethods = [];

            foreach ($reflectionClass->getMethods(ReflectionMethod::IS_PUBLIC) as $reflectionMethod) {
                // Trait methods report the using class as declaring class; only the file tells them apart.
                if (
                    str_starts_with($reflectionMethod->getName(), '__')
                    || $reflectionMethod->getDeclaringClass()->getName() !== $className
                    || $reflectionMethod->getFileName() !== $reflectionClass->getFileName()
                ) {
                    continue;
                }

                $method = MethodModel::fromReflection($reflectionMethod);

                foreach ($method->referencedTypes() as $type) {
                    if ($type->namespace !== null) {
                        $queue[] = ltrim($type->namespace . '\\' . $type->name, '\\');
                    }
                }

                if ($reflectionMethod->isStatic()) {
                    $staticMethods[] = $method;
                } else {
                    $methods[] = $method;
                }
            }

            $constants = $reflectionClass instanceof ReflectionEnum ? array_keys($reflectionClass->getCases()) : [];

            foreach ($reflectionClass->getConstants(ReflectionClassConstant::IS_PUBLIC) as $reflectionConstant) {
                if ($reflectionConstant->getDeclaringClass()->getName() === $className) {
                    $constants[] = $reflectionConstant->getName();
                }
            }

            $fileName = $reflectionClass->getFileName();
            $docComment = $reflectionClass->getDocComment();

            yield new ClassModel(
                $className,
                $fileName !== null ? $this->relativePath($fileName) : '',
                $supertypes,
                $methods,
                $staticMethods,
                $constants,
                $docComment !== null ? base64_encode($docComment) : null,
            );
        }
    }

    private function isExtractedType(string $className): bool
    {
        try {
            $reflectionClass = $this->reflector->reflectClass($className);
        } catch (IdentifierNotFound) {
            return false;
        }

        if ($reflectionClass->isInternal()) {
            return in_array($reflectionClass->getExtensionName(), $this->phpExtensions, true);
        }

        $fileName = $reflectionClass->getFileName();

        return (
            $fileName !== null
            && str_starts_with((string) realpath($fileName), $this->repositoryRootPath)
            && !str_contains($this->relativePath($fileName), 'vendor/')
        );
    }

    private function relativePath(string $fileName): string
    {
        return ltrim(str_replace($this->repositoryRootPath, '', (string) realpath($fileName)), '/');
    }
}
