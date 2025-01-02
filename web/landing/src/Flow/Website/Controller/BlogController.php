<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Blog\Posts;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class BlogController extends AbstractController
{
    #[Route('/blog/{date}/{slug}', name: 'blog_post')]
    public function post(string $date, string $slug) : Response
    {
        return $this->render('blog/posts/' . $date . '/' . $slug . '/post.html.twig', [
            'template_folder' => 'blog/posts/' . $date . '/' . $slug,
            'post' => (new Posts())->findByDateAndSlug($date, $slug),
            'date' => $date,
            'slug' => $slug,
        ]);
    }

    #[Route('/blog', name: 'blog', options: ['sitemap' => true])]
    public function posts() : Response
    {
        return $this->render('blog/posts.html.twig', [
            'posts' => (new Posts())->all(),
        ]);
    }
}
