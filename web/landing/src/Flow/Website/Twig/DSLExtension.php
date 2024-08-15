<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Twig\Extension\AbstractExtension;
use Twig\TwigFunction;

final class DSLExtension extends AbstractExtension
{
    public function __construct(private string $dslPath)
    {
    }

    public function dsl() : string
    {
        return file_get_contents($this->dslPath);
    }

    public function getFunctions()
    {
        return [
            new TwigFunction('dsl', [$this, 'dsl']),
        ];
    }
}
