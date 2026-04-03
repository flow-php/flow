<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Database;

interface CreateDatabaseOptionsStep extends CreateDatabaseFinalStep
{
    public function allowConnections(bool $allow) : self;

    public function builtinLocale(string $locale) : self;

    public function collationVersion(string $version) : self;

    public function connectionLimit(int $limit) : self;

    public function encoding(string $encoding) : self;

    public function icuLocale(string $locale) : self;

    public function icuRules(string $rules) : self;

    public function ifNotExists() : self;

    public function isTemplate(bool $template) : self;

    public function lcCollate(string $collate) : self;

    public function lcCtype(string $ctype) : self;

    public function locale(string $locale) : self;

    public function localeProvider(string $provider) : self;

    public function oid(int $oid) : self;

    public function owner(string $role) : self;

    public function strategy(string $strategy) : self;

    public function tablespace(string $name) : self;

    public function template(string $name) : self;
}
