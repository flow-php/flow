<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Examples;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class HomeController extends AbstractController
{
    public function __construct(
        private readonly Examples $examples,
    ) {}

    #[Route('/', name: 'home', options: ['sitemap' => true])]
    public function home(): Response
    {
        return $this->render('main/index.html.twig', [
            'topicsNavigation' => $this->examples->topicsNavigation(),
        ]);
    }

    #[Route('/404.html', name: 'not_found', options: ['sitemap' => false])]
    public function notFound(): Response
    {
        return $this->render('main/not_found.html.twig', []);
    }

    #[Route('/sponsor', name: 'sponsor', options: ['sitemap' => false])]
    public function sponsor(): Response
    {
        return $this->render('main/sponsor.html.twig', []);
    }
}
