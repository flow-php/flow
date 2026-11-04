<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function array_map;

final readonly class ClassModel
{
    /**
     * @param list<class-string> $supertypes parent, interfaces and traits
     * @param list<MethodModel> $methods instance methods declared in this type only
     * @param list<MethodModel> $staticMethods static methods declared in this type only
     * @param list<string> $constants public constants and enum cases declared in this type only
     */
    public function __construct(
        public string $name,
        public string $repositoryPath,
        public array $supertypes,
        public array $methods,
        public array $staticMethods,
        public array $constants,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'repository_path' => $this->repositoryPath,
            'supertypes' => $this->supertypes,
            'methods' => array_map(static fn(MethodModel $method): array => $method->normalize(), $this->methods),
            'static_methods' => array_map(
                static fn(MethodModel $method): array => $method->normalize(),
                $this->staticMethods,
            ),
            'constants' => $this->constants,
        ];
    }
}
