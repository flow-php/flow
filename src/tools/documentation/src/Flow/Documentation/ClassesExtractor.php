<?php

declare(strict_types=1);

namespace Flow\Documentation;

use Flow\Documentation\Models\ClassModel;
use Flow\Documentation\Models\MethodModel;
use Generator;
use ReflectionClass;
use ReflectionClassConstant;
use ReflectionMethod;

use function array_filter;
use function array_key_exists;
use function array_merge;
use function array_pop;
use function array_values;
use function class_exists;
use function enum_exists;
use function interface_exists;
use function ltrim;
use function str_contains;
use function str_replace;
use function str_starts_with;
use function trait_exists;

final readonly class ClassesExtractor
{
    public function __construct(
        private string $repositoryRootPath,
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

            if (array_key_exists($className, $extracted) || !$this->isRepositoryType($className)) {
                continue;
            }

            $extracted[$className] = true;
            $reflectionClass = new ReflectionClass($className);
            $parent = $reflectionClass->getParentClass();
            $supertypes = array_values(array_filter(
                array_merge(
                    $parent !== false ? [$parent->getName()] : [],
                    $reflectionClass->getInterfaceNames(),
                    $reflectionClass->getTraitNames(),
                ),
                $this->isRepositoryType(...),
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
                        $queue[] = $type->namespace . '\\' . $type->name;
                    }
                }

                if ($reflectionMethod->isStatic()) {
                    $staticMethods[] = $method;
                } else {
                    $methods[] = $method;
                }
            }

            $constants = [];

            foreach ($reflectionClass->getReflectionConstants(
                ReflectionClassConstant::IS_PUBLIC,
            ) as $reflectionConstant) {
                if ($reflectionConstant->getDeclaringClass()->getName() === $className) {
                    $constants[] = $reflectionConstant->getName();
                }
            }

            yield new ClassModel(
                $className,
                $this->relativePath((string) $reflectionClass->getFileName()),
                $supertypes,
                $methods,
                $staticMethods,
                $constants,
            );
        }
    }

    /**
     * @assert-if-true class-string $className
     */
    private function isRepositoryType(string $className): bool
    {
        if (
            !class_exists($className)
            && !interface_exists($className)
            && !trait_exists($className)
            && !enum_exists($className)
        ) {
            return false;
        }

        $fileName = (new ReflectionClass($className))->getFileName();

        return (
            $fileName !== false
            && str_starts_with($fileName, $this->repositoryRootPath)
            && !str_contains($this->relativePath($fileName), 'vendor/')
        );
    }

    private function relativePath(string $fileName): string
    {
        return ltrim(str_replace($this->repositoryRootPath, '', $fileName), '/');
    }
}
