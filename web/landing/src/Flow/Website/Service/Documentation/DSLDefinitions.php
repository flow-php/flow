<?php

declare(strict_types=1);

namespace Flow\Website\Service\Documentation;

use Flow\Website\Model\Documentation\{DSLDefinition, Module, Type};

final class DSLDefinitions
{
    /**
     * @param array<DSLDefinition> $definitions
     */
    private function __construct(private readonly array $definitions)
    {
    }

    public static function fromJson(string $definitionsPath) : self
    {
        return new self(\json_decode(\file_get_contents($definitionsPath), true, 512, JSON_THROW_ON_ERROR));
    }

    /**
     * @return array<DSLDefinition>
     */
    public function all() : array
    {
        $definitions = \array_map(
            fn (array $data) => new DSLDefinition($data),
            $this->definitions
        );

        \usort($definitions, fn (DSLDefinition $a, DSLDefinition $b) => \strnatcasecmp($a->name(), $b->name()));

        return $definitions;
    }

    public function count() : int
    {
        return \count($this->definitions);
    }

    public function fromModule(Module $module) : self
    {
        $definitions = [];

        foreach ($this->all() as $definition) {
            if (!$definition->module()) {
                continue;
            }

            if ($definition->module() === $module) {
                $definitions[] = $definition->data();
            }
        }

        return new self($definitions);
    }

    public function get(string $slug) : ?DSLDefinition
    {
        foreach ($this->all() as $definition) {
            if ($definition->slug() === $slug) {
                return $definition;
            }
        }

        return null;
    }

    /**
     * @return array<Module>
     */
    public function modules() : array
    {
        $modules = [];

        foreach ($this->all() as $definition) {
            $module = $definition->module();

            if ($module !== null && !\in_array($module, $modules, true)) {
                $modules[] = $module;
            }
        }

        uasort($modules, fn (Module $a, Module $b) => $a->priority() <=> $b->priority());

        return $modules;
    }

    public function onlyType(?Type $type) : self
    {
        $definitions = [];

        foreach ($this->all() as $definition) {
            if ($definition->type() === $type) {
                $definitions[] = $definition->data();
            }
        }

        return new self($definitions);
    }

    public function types() : array
    {
        $types = [];

        foreach ($this->all() as $definition) {
            $type = $definition->type();

            if ($type !== null && !\in_array($type, $types, true)) {
                $types[] = $type;
            }
        }

        uasort($types, fn (Type $a, Type $b) => $a->priority() <=> $b->priority());

        return $types;
    }
}
