<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Symfony\Component\String\Slugger\AsciiSlugger;
use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

final class SlugifyExtension extends AbstractExtension
{
    #[\Override]
    public function getFilters(): array
    {
        return [
            new TwigFilter('slugify', $this->slugify(...)),
        ];
    }

    public function slugify(string $text): string
    {
        return (new AsciiSlugger())
            ->slug($text)
            ->lower()
            ->toString();
    }
}
