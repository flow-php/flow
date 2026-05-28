<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Documentation\DSLDefinitions;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use NorbertTech\StaticContentGeneratorBundle\Content\SourceProvider;
use RuntimeException;

final readonly class DSLProvider implements SourceProvider
{
    public function __construct(
        private DSLDefinitions $dslDefinitions,
    ) {}

    public function all(): array
    {
        $sources = [];

        foreach ($this->dslDefinitions->modules() as $module) {
            $sources[] = new Source('documentation_dsl', ['module' => mb_strtolower($module->name)]);
        }

        foreach ($this->dslDefinitions->all() as $definition) {
            $module = $definition->module();

            if ($module === null) {
                throw new RuntimeException(
                    'Module is required for DSL definition, non given for: ' . $definition->path(),
                );
            }

            $sources[] = new Source('documentation_dsl_function', [
                'module' => mb_strtolower($module->name),
                'function' => $definition->slug(),
            ]);
        }

        return $sources;
    }
}
