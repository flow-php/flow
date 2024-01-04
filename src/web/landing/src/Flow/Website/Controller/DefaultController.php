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
        return $this->render('main/index.html.twig');
    }
}
