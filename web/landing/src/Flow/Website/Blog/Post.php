<?php

declare(strict_types=1);

namespace Flow\Website\Blog;

use function Flow\Types\DSL\{type_string, type_structure};

final class Post
{
    public function __construct(
        public readonly string $title,
        public readonly string $description,
        public readonly \DateTimeImmutable $date,
        public readonly string $slug,
    ) {
    }

    public static function fromArray(array $data) : self
    {
        type_structure([
            'title' => type_string(),
            'description' => type_string(),
            'date' => type_string(),
            'slug' => type_string(),
        ])->assert($data);

        return new self(
            $data['title'],
            $data['description'],
            new \DateTimeImmutable($data['date']),
            $data['slug']
        );
    }
}
