<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Documentation\Pages;
use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};

final class DocumentationProvider implements SourceProvider
{
    public function __construct(
        private readonly Pages $pages,
    ) {

    }

    public function all() : array
    {
        foreach ($this->pages->all() as $page) {
            $sources[] = new Source('documentation_page', ['path' => $page->path]);
        }

        return $sources;
    }
}
