<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class BlogController extends AbstractController
{
    public function __construct()
    {
    }

    #[Route('/blog/{date}/{slug}', name: 'blog_post')]
    public function post(string $date, string $slug) : Response
    {
        return $this->render('blog/' . $date . '/' . $slug . '/post.html.twig', [
            'template_folder' => 'blog/' . $date . '/' . $slug,
        ]);
    }
}
