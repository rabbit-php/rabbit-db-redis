<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Pool;

use Rabbit\Pool\PoolProperties;

/**
 * Class RedisPoolConfig
 * @package Rabbit\DB\Redis\Pool
 */
class RedisPoolConfig extends PoolProperties
{
    protected int $db = 0;

    protected string $prefix = '';

    protected bool $isSerialize = false;

    public function getSerialize(): bool
    {
        return $this->isSerialize;
    }

    public function getDb(): int
    {
        return $this->db;
    }

    public function getPrefix(): string
    {
        return $this->prefix;
    }
}
