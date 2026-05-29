<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Documentation\Pages;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use NorbertTech\StaticContentGeneratorBundle\Content\SourceProvider;

final readonly class DocumentationProvider implements SourceProvider
{
    public function __construct(
        private Pages $pages,
    ) {}

    public function all(): array
    {
        $sources = [];

        foreach ($this->pages->all() as $page) {
            $sources[] = new Source('documentation_page', ['path' => $page->path]);
        }

        return $sources;
    }
}
