<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\{Examples};
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class HomeController extends AbstractController
{
    public function __construct(
        private readonly Examples $examples,
    ) {
    }

    #[Route('/cookies', name: 'cookies', options: ['sitemap' => false])]
    public function cookies() : Response
    {
        return $this->render('main/cookies.html.twig', [
        ]);
    }

    #[Route('/', name: 'home', options: ['sitemap' => true])]
    public function home() : Response
    {
        return $this->render('main/index.html.twig', [
            'topics' => $this->examples->topics(),
        ]);
    }
}
