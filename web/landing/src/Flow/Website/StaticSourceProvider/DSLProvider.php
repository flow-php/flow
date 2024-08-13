<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Cocur\Slugify\Slugify;
use Flow\Website\Service\Documentation\DSLDefinitions;
use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};

final class DSLProvider implements SourceProvider
{
    public function __construct(private readonly DSLDefinitions $dslDefinitions)
    {
    }

    public function all() : array
    {
        $sources = [];

        foreach ($this->dslDefinitions->modules() as $module) {
            $sources[] = new Source('documentation_dsl', ['module' => (new Slugify())->slugify($module)]);
        }

        return $sources;
    }
}
