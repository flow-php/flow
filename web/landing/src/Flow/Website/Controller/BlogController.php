<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

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
        ]);
    }

    #[Route('/blog', name: 'blog', priority: 100)]
    public function posts() : Response
    {
        $posts = [
            [
                'title' => 'Building Custom Data Extractor - Flow PHP',
                'description' => 'Learn how to extract data from Google Analytics API using Flow PHP but also how to build a custom data extractor.',
                'date' => '2024-04-04',
                'slug' => 'building-custom-extractor-google-analytics',
            ],
        ];

        return $this->render('blog/posts.html.twig', [
            'posts' => $posts,
        ]);
    }
}
