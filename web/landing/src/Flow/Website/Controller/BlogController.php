<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Blog\Posts;
use Flow\Website\SocialCard\Blog\CardGenerator;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class BlogController extends AbstractController
{
    #[Route('/blog/{date}/{slug}', name: 'blog_post')]
    public function post(string $date, string $slug, CardGenerator $socialCards): Response
    {
        $post = (new Posts())->findByDateAndSlug($date, $slug);

        return $this->render('blog/posts/' . $date . '/' . $slug . '/post.html.twig', [
            'template_folder' => 'blog/posts/' . $date . '/' . $slug,
            'post' => $post,
            'date' => $date,
            'slug' => $slug,
            'social_image' => $socialCards->generate($post),
        ]);
    }

    #[Route('/blog', name: 'blog', options: ['sitemap' => true])]
    public function posts(): Response
    {
        return $this->render('blog/posts.html.twig', [
            'posts' => (new Posts())->all(),
        ]);
    }

    #[Route('/rss.xml', name: 'blog_rss')]
    public function rss(): Response
    {
        $response = $this->render('blog/rss.xml.twig', [
            'posts' => (new Posts())->all(),
        ]);
        $response->headers->set('Content-Type', 'application/rss+xml; charset=UTF-8');

        return $response;
    }
}
