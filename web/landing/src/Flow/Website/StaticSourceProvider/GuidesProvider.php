<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Documentation\Pages;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use NorbertTech\StaticContentGeneratorBundle\Content\SourceProvider;
use Symfony\Component\DependencyInjection\Attribute\Autowire;

final readonly class GuidesProvider implements SourceProvider
{
    public function __construct(
        #[Autowire(service: 'guides.pages')]
        private Pages $guides,
    ) {}

    public function all(): array
    {
        $sources = [];

        foreach ($this->guides->all() as $guide) {
            $sources[] = new Source('guides_guide', ['slug' => $guide->path]);
        }

        return $sources;
    }
}
