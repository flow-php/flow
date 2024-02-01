<?php declare(strict_types=1);

namespace Flow\Website\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

class DefaultController extends AbstractController
{
    #[Route('/', name: 'main')]
    public function main() : Response
    {
        $contributors = [
            [
                'url' => 'https://github.com/norberttech',
                'avatar' => 'https://avatars.githubusercontent.com/u/1921950?s=60&amp;v=4',
                'name' => 'norberttech',
            ],
            [
                'url' => 'https://github.com/stloyd',
                'avatar' => 'https://avatars.githubusercontent.com/u/67402?s=60&amp;v=4',
                'name' => 'stloyd',
            ],
            [
                'url' => 'https://github.com/tomaszhanc',
                'avatar' => 'https://avatars.githubusercontent.com/u/7013293?s=60&amp;v=4',
                'name' => 'tomaszhanc',
            ],
            [
                'url' => 'https://github.com/DawidSajdak',
                'avatar' => 'https://avatars.githubusercontent.com/u/946972?s=60&amp;v=4',
                'name' => 'DawidSajdak',
            ],
            [
                'url' => 'https://github.com/rzarno',
                'avatar' => 'https://avatars.githubusercontent.com/u/12570337?s=60&amp;v=4',
                'name' => 'rzarno',
            ],
            [
                'url' => 'https://github.com/owsiakl',
                'avatar' => 'https://avatars.githubusercontent.com/u/9623965?s=60&amp;v=4',
                'name' => 'owsiakl',
            ],
            [
                'url' => 'https://github.com/norbertmwk',
                'avatar' => 'https://avatars.githubusercontent.com/u/82225968?s=60&amp;v=4',
                'name' => 'norbertmwk',
            ],
            [
                'url' => 'https://github.com/szepeviktor',
                'avatar' => 'https://avatars.githubusercontent.com/u/952007?s=60&amp;v=4',
                'name' => 'szepeviktor',
            ],
            [
                'url' => 'https://github.com/scyzoryck',
                'avatar' => 'https://avatars.githubusercontent.com/u/8014727?s=60&amp;v=4',
                'name' => 'scyzoryck',
            ],
            [
                'url' => 'https://github.com/xaviermarchegay',
                'avatar' => 'https://avatars.githubusercontent.com/u/658523?s=60&amp;v=4',
                'name' => 'xaviermarchegay',
            ],
            [
                'url' => 'https://github.com/Wiktor6',
                'avatar' => 'https://avatars.githubusercontent.com/u/24683748?s=60&amp;v=4',
                'name' => 'Wiktor6',
            ],
            [
                'url' => 'https://github.com/peter279k',
                'avatar' => 'https://avatars.githubusercontent.com/u/9021747?s=60&amp;v=4',
                'name' => 'peter279k',
            ],
            [
                'url' => 'https://github.com/saulblake',
                'avatar' => 'https://avatars.githubusercontent.com/u/582274?s=60&amp;v=4',
                'name' => 'saulblake',
            ],
            [
                'url' => 'https://github.com/jpiatko',
                'avatar' => 'https://avatars.githubusercontent.com/u/80686947?s=60&amp;v=4',
                'name' => 'jpiatko',
            ],
            [
                'url' => 'https://github.com/drupol',
                'avatar' => 'https://avatars.githubusercontent.com/u/252042?s=60&amp;v=4',
                'name' => 'drupol',
            ],
            [
                'url' => 'https://github.com/Wirone',
                'avatar' => 'https://avatars.githubusercontent.com/u/600668?s=60&amp;v=4',
                'name' => 'Wirone',
            ],
            [
                'url' => 'https://github.com/mleczakm',
                'avatar' => 'https://avatars.githubusercontent.com/u/3474636?s=60&amp;v=4',
                'name' => 'mleczakm',
            ],
            [
                'url' => 'https://github.com/flavioheleno',
                'avatar' => 'https://avatars.githubusercontent.com/u/471860?s=60&amp;v=4',
                'name' => 'flavioheleno',
            ],
            [
                'url' => 'https://github.com/voku',
                'avatar' => 'https://avatars.githubusercontent.com/u/264695?s=60&amp;v=4',
                'name' => 'voku',
            ],
            [
                'url' => 'https://github.com/FunkyOz',
                'avatar' => 'https://avatars.githubusercontent.com/u/26649880?s=60&amp;v=4',
                'name' => 'FunkyOz',
            ],
        ];

        return $this->render('main/index.html.twig', [
            'contributors' => $contributors,
        ]);
    }
}
