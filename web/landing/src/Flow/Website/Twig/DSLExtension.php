<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Override;
use Twig\Extension\AbstractExtension;
use Twig\TwigFunction;

use function Flow\Types\DSL\type_string;

final class DSLExtension extends AbstractExtension
{
    public function __construct(
        private readonly string $dslPath,
    ) {}

    public function dsl(): string
    {
        return type_string()->assert(file_get_contents($this->dslPath));
    }

    #[Override]
    public function getFunctions()
    {
        return [
            new TwigFunction('dsl', $this->dsl(...)),
        ];
    }
}
