<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class PlaygroundController extends AbstractController
{
    public function __construct() {}

    #[Route('/playground', name: 'playground')]
    public function index(): Response
    {
        return $this->render('playground/index.html.twig', []);
    }
}
