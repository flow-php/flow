<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Github;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;

final class LayoutController extends AbstractController
{
    public function __construct(
        private readonly Github $github,
    ) {
    }

    public function contributors() : Response
    {
        return $this->render('main/_contributors.html.twig', [
            'contributors' => $this->github->contributors(),
        ]);
    }

    public function hero() : Response
    {
        return $this->render('main/_hero.html.twig', [
            'version' => $this->github->version('flow-php/flow'),
        ]);
    }
}
