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

        return $html;
    }
}
