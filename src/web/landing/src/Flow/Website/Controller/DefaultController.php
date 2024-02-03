<?php declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Github;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DefaultController extends AbstractController
{
    public function __construct(private readonly Github $github)
    {
    }

    #[Route('/', name: 'main')]
    public function main() : Response
    {
        return $this->render('main/index.html.twig', [
            'contributors' => $this->github->contributors(),
        ]);
    }
}
