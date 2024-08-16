<?php

declare(strict_types=1);

namespace Flow\Website\Service\Documentation;

use Cocur\Slugify\Slugify;
use Flow\Website\Model\Documentation\DSLDefinition;

final class DSLDefinitions
{
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

    public function fromModule(string $module) : self
    {
        $definitions = [];

        foreach ($this->all() as $definition) {
            if (!$definition->module()) {
                continue;
            }

            if ((new Slugify())->slugify($definition->module()) === (new Slugify())->slugify($module)) {
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
     * @return array<string>
     */
    public function modules() : array
    {
        $modules = [];

        foreach ($this->all() as $definition) {
            $module = $definition->module();

            if ($module !== null) {
                $modules[] = $module;
            }
        }

        $modules = \array_unique(\array_filter($modules));
        \sort($modules);

        $sortedModules = [
            'Core',
            'CSV',
            'Doctrine',
            'Elastic Search',
            'Google Sheet',
            'ChartJS',
            'JSON',
            'MeiliSearch',
            'Parquet',
            'Text',
            'XML',
            'Filesystem',
            'Azure Filesystem',
            'Azure SDK',
        ];

        return \array_values(\array_intersect($sortedModules, $modules));
    }

    public function onlyType(?string $type) : self
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
            $types[] = $definition->type();
        }

        $types = \array_unique(\array_filter($types));
        \sort($types);
        $sortedTypes = [
            'data frame',
            'extractors',
            'loaders',
            'helpers',
            'entries',
            'types',
            'schema',
            'aggregating functions',
            'scalar functions',
            'window functions',
            'comparisons',
            'transformers',
        ];

        $types = \array_values(\array_intersect($sortedTypes, $types));

        return $types;
    }
}
