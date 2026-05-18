<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

use SensitiveParameter;

interface CreateRoleOptionsStep extends CreateRoleFinalStep
{
    public function bypassRls(): self;

    public function connectionLimit(int $limit): self;

    public function createDb(): self;

    public function createRole(): self;

    public function inherit(): self;

    public function inRole(string ...$roles): self;

    public function login(): self;

    public function noBypassRls(): self;

    public function noCreateDb(): self;

    public function noCreateRole(): self;

    public function noInherit(): self;

    public function noLogin(): self;

    public function noReplication(): self;

    public function noSuperuser(): self;

    public function replication(): self;

    public function superuser(): self;

    public function validUntil(string $timestamp): self;

    public function withPassword(#[SensitiveParameter] string $password): self;
}
