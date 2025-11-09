<?php

declare(strict_types=1);

namespace Flow\Website\Twig;

use Symfony\Component\AssetMapper\ImportMap\ImportMapRenderer;
use Twig\Extension\AbstractExtension;
use Twig\TwigFunction;

final class ImportMapExtension extends AbstractExtension
{
    public function __construct(private readonly ImportMapRenderer $importMapRenderer)
    {
    }

    public function getFunctions() : array
    {
        return [
            new TwigFunction('importmap', $this->renderImportmap(...), ['is_safe' => ['html']]),
        ];
    }

    public function renderImportmap(string $entryPoint = 'app') : string
    {
        $html = $this->importMapRenderer->render($entryPoint);

        // Add scopes to fix Stimulus controller relative imports
        $html = \str_replace(
            '    }
}',
            '    },
    "scopes": {
        "/assets/@symfony/stimulus-bundle/": {
            "../../controllers/": "/assets/controllers/"
        }
    }
}',
            $html
        );

        // Remove async from es-module-shims to prevent race condition
        // The polyfill must load before module scripts execute
        $html = \str_replace(
            '<script async src="https://ga.jspm.io/npm:es-module-shims',
            '<script src="https://ga.jspm.io/npm:es-module-shims',
            $html
        );

        return $html;
    }
}
