<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Cocur\Slugify\Slugify;
use Twig\Extension\AbstractExtension;
use Twig\TwigFilter;

final class SlugifyExtension extends AbstractExtension
{
    public function getFilters()
    {
        return [
            new TwigFilter('slugify', [$this, 'slugify']),
        ];
    }

    public function slugify(string $text) : string
    {
        return (new Slugify())->slugify($text);
    }
}
