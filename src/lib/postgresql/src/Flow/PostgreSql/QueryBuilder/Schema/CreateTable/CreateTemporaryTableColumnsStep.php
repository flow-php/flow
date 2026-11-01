<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTable;

interface CreateTemporaryTableColumnsStep extends CreateTableColumnsStep, CreateTableTemporaryStep {}
